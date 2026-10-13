package provider

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// Trakt apps created after its October 2026 PKCE change may have no client
// secret. Such an app signs in, refreshes, and syncs with its client ID alone,
// and no OAuth body carries an empty client_secret.
func TestAppWithoutClientSecretSignsInAndRefreshes(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	bodies := map[string]map[string]string{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("trakt-api-key") != testClientID {
			t.Errorf("%s trakt-api-key = %q", r.URL.Path, r.Header.Get("trakt-api-key"))
		}
		if r.Method == http.MethodPost {
			var body map[string]string
			decodeBody(t, r, &body)
			mu.Lock()
			bodies[r.URL.Path] = body
			mu.Unlock()
		}
		switch r.URL.Path {
		case deviceCodePath:
			writeFixture(t, w, `{"device_code":"device-code","user_code":"5055CC52","verification_url":"https://trakt.tv/activate","expires_in":600,"interval":5}`)
		case deviceTokenPath, "/oauth/token":
			writeFixture(t, w, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":86400}`)
		case "/users/settings":
			writeFixture(t, w, `{"user":{"username":"sean","ids":{"slug":"sean"}}}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	config := &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}}
	ctx := context.Background()

	start, _ := server.DeviceAuthorization().Start(ctx, &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{CapabilityId: capabilityID, ProviderConfig: config})
	if start.GetFault() != nil || start.GetUserCode() != "5055CC52" {
		t.Fatalf("start = %v", start)
	}
	poll, _ := server.DeviceAuthorization().Poll(ctx, &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
		CapabilityId: capabilityID, ProviderConfig: config, ProviderState: start.GetProviderState(),
	})
	if poll.GetStatus() != pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED {
		t.Fatalf("poll = %v", poll)
	}
	auth := authContext(testAccessToken)
	auth.ProviderConfig = config
	refresh, _ := server.RefreshCredentials(ctx, &pluginv1.WatchSyncRefreshCredentialsRequest{Context: auth})
	if refresh.GetFault() != nil || refresh.GetCredentials().GetAccessToken() != "new-access" {
		t.Fatalf("refresh = %v", refresh)
	}
	account, _ := server.GetAccount(ctx, &pluginv1.WatchSyncGetAccountRequest{Context: auth})
	if account.GetFault() != nil || account.GetAccount().GetExternalSubject() != "sean" {
		t.Fatalf("account = %v", account)
	}

	want := map[string]map[string]string{
		deviceCodePath:  {"client_id": testClientID},
		deviceTokenPath: {"code": "device-code", "client_id": testClientID},
		"/oauth/token":  {"refresh_token": testRefreshToken, "client_id": testClientID, "grant_type": "refresh_token"},
	}
	for path, wantBody := range want {
		if got := bodies[path]; !maps.Equal(got, wantBody) {
			t.Errorf("%s body = %v, want %v", path, got, wantBody)
		}
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
