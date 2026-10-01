package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	deviceCodePath  = "/oauth/device/code"
	deviceTokenPath = "/oauth/device/token"

	// slowDownStep is how much a slow_down answer lengthens the polling
	// interval, as RFC 8628 prescribes.
	slowDownStep = 5 * time.Second
)

// deviceAuthorization implements Trakt's OAuth device-code sign-in.
type deviceAuthorization struct {
	pluginv1.UnimplementedWatchSyncDeviceAuthorizationServiceServer
	server *Server
}

// deviceState is the host-encrypted flow state between polls.
type deviceState struct {
	DeviceCode      string `json:"device_code"`
	IntervalSeconds int    `json:"interval_seconds"`
}

func (d *deviceAuthorization) Start(ctx context.Context, req *pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest) (*pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse, error) {
	client, fault := d.server.appClient(req.GetCapabilityId(), req.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: fault}, nil
	}
	ctx, cancel, _ := d.server.window(ctx)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"client_id": client.app.clientID})
	var code traktDeviceCode
	result, fault := client.roundTrip(ctx, http.MethodPost, deviceCodePath, nil, body, &code)
	switch {
	case fault != nil:
	case result.status == http.StatusTooManyRequests:
		fault = rateLimitedFault(result.retryAfter())
	case result.failed():
		fault = responseFault(http.MethodPost, deviceCodePath, false, result)
	case code.DeviceCode == "" || code.UserCode == "" || code.VerificationURL == "" || code.ExpiresIn <= 0 || code.Interval <= 0:
		fault = temporaryFault("Trakt returned an incomplete device code")
	}
	if fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{Fault: fault}, nil
	}
	state, _ := json.Marshal(deviceState{DeviceCode: code.DeviceCode, IntervalSeconds: code.Interval})
	return &pluginv1.WatchSyncDeviceAuthorizationServiceStartResponse{
		UserCode:        code.UserCode,
		VerificationUrl: code.VerificationURL,
		ProviderState:   state,
		PollingInterval: durationpb.New(time.Duration(code.Interval) * time.Second),
		ExpiresAt:       timestamppb.New(d.server.clock().UTC().Add(time.Duration(code.ExpiresIn) * time.Second)),
	}, nil
}

// Poll asks Trakt whether the user approved the code. Trakt answers a pending
// code with 400, polling too fast with 429, an expired code with 410, and a
// denied one with 418; RFC 8628 error codes in the body are read as well.
func (d *deviceAuthorization) Poll(ctx context.Context, req *pluginv1.WatchSyncDeviceAuthorizationServicePollRequest) (*pluginv1.WatchSyncDeviceAuthorizationServicePollResponse, error) {
	client, fault := d.server.appClient(req.GetCapabilityId(), req.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: fault}, nil
	}
	var state deviceState
	if err := json.Unmarshal(req.GetProviderState(), &state); err != nil || strings.TrimSpace(state.DeviceCode) == "" {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: invalidRequestFault("Trakt device authorization state is invalid")}, nil
	}
	ctx, cancel, _ := d.server.window(ctx)
	defer cancel()
	body, _ := json.Marshal(map[string]string{
		"code":          state.DeviceCode,
		"client_id":     client.app.clientID,
		"client_secret": client.app.clientSecret,
	})
	var token traktToken
	result, fault := client.roundTrip(ctx, http.MethodPost, deviceTokenPath, nil, body, &token)
	if fault != nil {
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: fault}, nil
	}
	if !result.failed() {
		credentials := d.server.credentials(token, "")
		if credentials == nil {
			return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: temporaryFault("Trakt returned no access token")}, nil
		}
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
			Status:      pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED,
			Credentials: credentials,
		}, nil
	}
	code := oauthErrorCode(result.errorBody)
	switch {
	case code == "authorization_pending", result.status == http.StatusBadRequest && code == "":
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING), nil
	case code == "slow_down", result.status == http.StatusTooManyRequests:
		return slowDown(state, result), nil
	case code == "expired_token", code == "invalid_grant",
		result.status == http.StatusGone, result.status == http.StatusNotFound, result.status == http.StatusConflict:
		// A code that is unknown (404) or already used (409) cannot complete
		// either; the user has to start over.
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED), nil
	case code == "access_denied", result.status == http.StatusTeapot:
		return pollStatus(pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED), nil
	default:
		return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Fault: responseFault(http.MethodPost, deviceTokenPath, false, result)}, nil
	}
}

func pollStatus(status pluginv1.WatchSyncDeviceAuthorizationStatus) *pluginv1.WatchSyncDeviceAuthorizationServicePollResponse {
	return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{Status: status}
}

// slowDown keeps the flow pending with a longer polling interval: five
// seconds more, or Trakt's Retry-After when that is longer. The interval is
// stored in the flow state so the next slow_down lengthens it again.
func slowDown(state deviceState, result httpResult) *pluginv1.WatchSyncDeviceAuthorizationServicePollResponse {
	interval := time.Duration(max(state.IntervalSeconds, 1))*time.Second + slowDownStep
	if wait, ok := parseRetryAfter(result.header.Get("Retry-After"), result.now); ok && wait > interval {
		interval = wait
	}
	state.IntervalSeconds = int((interval + time.Second - 1) / time.Second)
	encoded, _ := json.Marshal(state)
	return &pluginv1.WatchSyncDeviceAuthorizationServicePollResponse{
		Status:          pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING,
		ProviderState:   encoded,
		PollingInterval: durationpb.New(time.Duration(state.IntervalSeconds) * time.Second),
	}
}
