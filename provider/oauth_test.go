package provider

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func TestInitAuthorizeBuildsAPKCEURLWithoutASecret(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "1.2.3")
	config := &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}}
	response, err := server.InitAuthorize(context.Background(), &pluginv1.WatchSyncInitAuthorizeRequest{
		CapabilityId:   capabilityID,
		ProviderConfig: config,
		RedirectUri:    "https://silo.example/api/v2/watch-providers/oauth/callback",
		State:          "state-1",
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("init = %v, fault %v", err, response.GetFault())
	}
	parsed, err := url.Parse(response.GetAuthorizationUrl())
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if parsed.Scheme != "https" || parsed.Host != "trakt.tv" || parsed.Path != "/oauth/authorize" {
		t.Fatalf("url = %s", response.GetAuthorizationUrl())
	}
	if query.Get("client_id") != testClientID || query.Get("response_type") != "code" ||
		query.Get("redirect_uri") != "https://silo.example/api/v2/watch-providers/oauth/callback" ||
		query.Get("state") != "state-1" || query.Get("code_challenge_method") != "S256" || query.Get("client_secret") != "" {
		t.Fatalf("query = %v", query)
	}
	var state oauthFlowState
	if err := json.Unmarshal(response.GetProviderState(), &state); err != nil || !validVerifier(state.Verifier) {
		t.Fatalf("state = %s, %v", response.GetProviderState(), err)
	}
	sum := sha256.Sum256([]byte(state.Verifier))
	if query.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(sum[:]) {
		t.Fatalf("challenge does not match the verifier")
	}
}

func TestInitAuthorizeRejectsANonHTTPSRedirect(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "")
	response, _ := server.InitAuthorize(context.Background(), &pluginv1.WatchSyncInitAuthorizeRequest{
		CapabilityId:   capabilityID,
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}},
		RedirectUri:    "urn:ietf:wg:oauth:2.0:oob",
		State:          "state-1",
	})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v", response.GetFault())
	}
}

func TestExchangeCodeSendsTheVerifierAndOmitsAnEmptySecret(t *testing.T) {
	t.Parallel()
	var gotBody map[string]string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tokenPath {
			t.Errorf("path = %s", r.URL.Path)
		}
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":86400}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	state, _ := json.Marshal(oauthFlowState{Verifier: stringsRepeatVerifier()})
	response, err := server.ExchangeCode(context.Background(), &pluginv1.WatchSyncExchangeCodeRequest{
		CapabilityId:      capabilityID,
		ProviderConfig:    &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}},
		RedirectUri:       "https://silo.example/api/v2/watch-providers/oauth/callback",
		AuthorizationCode: "auth-code",
		ProviderState:     state,
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("exchange = %v, fault %v", err, response.GetFault())
	}
	if _, sent := gotBody["client_secret"]; sent || gotBody["code_verifier"] != stringsRepeatVerifier() ||
		gotBody["grant_type"] != "authorization_code" || gotBody["code"] != "auth-code" || gotBody["client_id"] != testClientID {
		t.Fatalf("body = %v", gotBody)
	}
	credentials := response.GetCredentials()
	if credentials.GetAccessToken() != "new-access" || credentials.GetRefreshToken() != "new-refresh" ||
		credentials.GetSecretAttributes()["redirect_uri"] != "https://silo.example/api/v2/watch-providers/oauth/callback" {
		t.Fatalf("credentials = %v", credentials)
	}
}

func TestExchangeCodeSendsALegacySecret(t *testing.T) {
	t.Parallel()
	var gotBody map[string]string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":86400}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	state, _ := json.Marshal(oauthFlowState{Verifier: stringsRepeatVerifier()})
	response, err := server.ExchangeCode(context.Background(), &pluginv1.WatchSyncExchangeCodeRequest{
		CapabilityId:      capabilityID,
		ProviderConfig:    providerConfig(),
		RedirectUri:       "https://silo.example/cb",
		AuthorizationCode: "auth-code",
		ProviderState:     state,
	})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("exchange = %v, fault %v", err, response.GetFault())
	}
	if gotBody["client_secret"] != testClientSecret {
		t.Fatalf("body = %v", gotBody)
	}
}

func TestRefreshOmitsAnEmptySecretAndSendsTheStoredRedirect(t *testing.T) {
	t.Parallel()
	var gotBody map[string]string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decodeBody(t, r, &gotBody)
		writeFixture(t, w, `{"access_token":"next-access","refresh_token":"next-refresh","expires_in":86400}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	auth := authContext(testAccessToken)
	auth.ProviderConfig = &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}}
	auth.Credentials.SecretAttributes = map[string]string{"redirect_uri": "https://silo.example/cb"}
	response, err := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: auth})
	if err != nil || response.GetFault() != nil {
		t.Fatalf("refresh = %v, fault %v", err, response.GetFault())
	}
	if _, sent := gotBody["client_secret"]; sent || gotBody["redirect_uri"] != "https://silo.example/cb" || gotBody["grant_type"] != "refresh_token" {
		t.Fatalf("body = %v", gotBody)
	}
	if response.GetCredentials().GetSecretAttributes()["redirect_uri"] != "https://silo.example/cb" {
		t.Fatalf("credentials = %v", response.GetCredentials())
	}
}

func TestDeviceSignInWithoutASecretFailsBeforeCallingTrakt(t *testing.T) {
	t.Parallel()
	var requests int
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	config := &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: testClientID}}
	device := server.DeviceAuthorization()
	start, _ := device.Start(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{CapabilityId: capabilityID, ProviderConfig: config})
	poll, _ := device.Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
		CapabilityId: capabilityID, ProviderConfig: config, ProviderState: []byte(`{"device_code":"x"}`),
	})
	for _, fault := range []*pluginv1.WatchSyncFault{start.GetFault(), poll.GetFault()} {
		if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED || fault.GetSafeMessage() != deviceSecretMessage {
			t.Fatalf("fault = %v", fault)
		}
	}
	if requests != 0 {
		t.Fatalf("upstream saw %d requests", requests)
	}
}

func stringsRepeatVerifier() string {
	return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
}
