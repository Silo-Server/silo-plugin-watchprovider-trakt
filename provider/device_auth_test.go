package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func startRequest() *pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest {
	return &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{CapabilityId: capabilityID, ProviderConfig: providerConfig()}
}

func pollRequest(state deviceState) *pluginv1.WatchSyncDeviceAuthorizationServicePollRequest {
	encoded, _ := json.Marshal(state)
	return &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{CapabilityId: capabilityID, ProviderConfig: providerConfig(), ProviderState: encoded}
}

func TestStartSendsTraktHeadersAndReturnsTheUserChallenge(t *testing.T) {
	t.Parallel()
	var gotPath string
	var gotHeader http.Header
	var gotBody map[string]string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotHeader = r.URL.Path, r.Header.Clone()
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"device_code":"device-code","user_code":"USER1234","verification_url":"https://trakt.tv/activate","expires_in":600,"interval":5}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	response, err := server.DeviceAuthorization().Start(context.Background(), startRequest())
	if err != nil || response.GetFault() != nil {
		t.Fatalf("Start = %v, %v", response, err)
	}
	if gotPath != "/oauth/device/code" || gotBody["client_id"] != testClientID || len(gotBody) != 1 {
		t.Fatalf("request = %s %v", gotPath, gotBody)
	}
	if gotHeader.Get("trakt-api-key") != testClientID || gotHeader.Get("trakt-api-version") != "2" ||
		!strings.HasPrefix(gotHeader.Get("User-Agent"), "silo-plugin-watchprovider-trakt/") || gotHeader.Get("Authorization") != "" {
		t.Fatalf("headers = %v", gotHeader)
	}
	if response.GetUserCode() != "USER1234" || response.GetVerificationUrl() != "https://trakt.tv/activate" {
		t.Fatalf("challenge = %v", response)
	}
	if response.GetPollingInterval().AsDuration() != 5*time.Second || !response.GetExpiresAt().AsTime().Equal(now.Add(10*time.Minute)) {
		t.Fatalf("timing = %v, %v", response.GetPollingInterval(), response.GetExpiresAt())
	}
	var state deviceState
	if err := json.Unmarshal(response.GetProviderState(), &state); err != nil || state.DeviceCode != "device-code" || state.IntervalSeconds != 5 {
		t.Fatalf("provider state = %s", response.GetProviderState())
	}
}

func TestStartRejectsAnIncompleteResponse(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeFixture(t, w, `{"device_code":"device-code","expires_in":600,"interval":5}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	response, _ := server.DeviceAuthorization().Start(context.Background(), startRequest())
	if response.GetFault() == nil || response.GetUserCode() != "" {
		t.Fatalf("response = %v, want a fault", response)
	}
}

func TestStartReportsRateLimits(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	response, _ := server.DeviceAuthorization().Start(context.Background(), startRequest())
	if wait := requireRateLimited(t, response.GetFault()); wait != 30*time.Second {
		t.Fatalf("retry after = %s, want 30s", wait)
	}
}

func TestPollMapsTraktDeviceTokenAnswers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantStatus pluginv1.WatchSyncDeviceAuthorizationStatus
		wantFault  pluginv1.WatchSyncFaultCode
	}{
		{name: "pending", status: http.StatusBadRequest, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING},
		{name: "pending by code", status: http.StatusBadRequest, body: `{"error":"authorization_pending"}`, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING},
		{name: "expired", status: http.StatusGone, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{name: "expired by code", status: http.StatusBadRequest, body: `{"error":"expired_token"}`, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{name: "unknown code", status: http.StatusNotFound, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{name: "already used", status: http.StatusConflict, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{name: "denied", status: http.StatusTeapot, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED},
		{name: "denied by code", status: http.StatusBadRequest, body: `{"error":"access_denied"}`, wantStatus: pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED},
		{name: "rejected app", status: http.StatusUnauthorized, body: `{"error":"invalid_client"}`, wantFault: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED},
		{name: "outage", status: http.StatusBadGateway, wantFault: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			response, _ := server.DeviceAuthorization().Poll(context.Background(), pollRequest(deviceState{DeviceCode: "device-code", IntervalSeconds: 5}))
			if response.GetStatus() != tc.wantStatus || response.GetFault().GetCode() != tc.wantFault {
				t.Fatalf("poll = %v, want status %v fault %v", response, tc.wantStatus, tc.wantFault)
			}
			if response.GetCredentials() != nil || response.ProviderState != nil {
				t.Fatalf("poll = %v, want no credentials or state change", response)
			}
		})
	}
}

func TestPollSlowsDownEachTimeTraktAsks(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	state := deviceState{DeviceCode: "device-code", IntervalSeconds: 5}
	for _, want := range []time.Duration{10 * time.Second, 15 * time.Second} {
		response, _ := server.DeviceAuthorization().Poll(context.Background(), pollRequest(state))
		if response.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING ||
			response.GetFault() != nil || response.GetPollingInterval().AsDuration() != want {
			t.Fatalf("poll = %v, want pending every %s", response, want)
		}
		if err := json.Unmarshal(response.GetProviderState(), &state); err != nil || state.DeviceCode != "device-code" {
			t.Fatalf("provider state = %s", response.GetProviderState())
		}
	}
}

func TestPollAuthorizesWithCompleteCredentials(t *testing.T) {
	t.Parallel()
	var gotBody map[string]string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/device/token" {
			t.Errorf("path = %s", r.URL.Path)
		}
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":86400,"token_type":"bearer","scope":"public","created_at":1}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	response, _ := server.DeviceAuthorization().Poll(context.Background(), pollRequest(deviceState{DeviceCode: "device-code", IntervalSeconds: 5}))
	if response.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED {
		t.Fatalf("poll = %v", response)
	}
	if gotBody["code"] != "device-code" || gotBody["client_id"] != testClientID || gotBody["client_secret"] != testClientSecret {
		t.Fatalf("body = %v", gotBody)
	}
	credentials := response.GetCredentials()
	if credentials.GetAccessToken() != "new-access" || credentials.GetRefreshToken() != "new-refresh" ||
		!credentials.GetExpiresAt().AsTime().Equal(now.Add(24*time.Hour)) {
		t.Fatalf("credentials = %v", credentials)
	}
}

func TestPollRejectsUnreadableState(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "")
	for _, state := range [][]byte{nil, []byte("not json"), []byte(`{"interval_seconds":5}`)} {
		response, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
			CapabilityId: capabilityID, ProviderConfig: providerConfig(), ProviderState: state,
		})
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Fatalf("state %q: fault = %v", state, response.GetFault())
		}
	}
}

func TestRefreshReturnsTheRotatedTokens(t *testing.T) {
	t.Parallel()
	var gotBody map[string]string
	var gotAuthorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/oauth/token" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		gotAuthorization = r.Header.Get("Authorization")
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"access_token":"next-access","refresh_token":"next-refresh","expires_in":86400}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }

	response, err := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext(testAccessToken)})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("refresh = %v, %v", response, err)
	}
	want := map[string]string{"refresh_token": testRefreshToken, "client_id": testClientID, "client_secret": testClientSecret, "grant_type": "refresh_token"}
	if len(gotBody) != len(want) {
		t.Fatalf("body = %v, want %v", gotBody, want)
	}
	for key, value := range want {
		if gotBody[key] != value {
			t.Fatalf("body = %v, want %v", gotBody, want)
		}
	}
	if gotAuthorization != "" {
		t.Fatalf("Authorization = %q, want none on the OAuth endpoint", gotAuthorization)
	}
	credentials := response.GetCredentials()
	if credentials.GetAccessToken() != "next-access" || credentials.GetRefreshToken() != "next-refresh" ||
		!credentials.GetExpiresAt().AsTime().Equal(now.Add(24*time.Hour)) || len(credentials.GetSecretAttributes()) != 0 {
		t.Fatalf("credentials = %v", credentials)
	}
}

func TestRefreshKeepsTheRefreshTokenTraktDidNotReplace(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeFixture(t, w, `{"access_token":"next-access","expires_in":0}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	response, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext(testAccessToken)})
	credentials := response.GetCredentials()
	if credentials.GetAccessToken() != "next-access" || credentials.GetRefreshToken() != testRefreshToken || credentials.GetExpiresAt() != nil {
		t.Fatalf("credentials = %v", credentials)
	}
}

func TestRefreshWithoutARefreshTokenAsksForReconnect(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "")
	auth := authContext(testAccessToken)
	auth.Credentials.RefreshToken = ""
	response, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: auth})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL || response.GetCredentials() != nil {
		t.Fatalf("refresh = %v", response)
	}
}
