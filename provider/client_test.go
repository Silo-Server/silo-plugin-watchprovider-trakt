package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func rateLimitedUpstream(t *testing.T, retryAfter string, attempts *atomic.Int32) *httptest.Server {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(upstream.Close)
	return upstream
}

func progressTraversal(t *testing.T, server *Server) *pluginv1.WatchSyncFault {
	t.Helper()
	return traverse(t, server, pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS).fault
}

func requireRateLimited(t *testing.T, f *pluginv1.WatchSyncFault) time.Duration {
	t.Helper()
	if f.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		t.Fatalf("fault = %v, want RATE_LIMITED", f)
	}
	return f.GetRetryAfter().AsDuration()
}

func TestRateLimitUsesRetryAfterSeconds(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server, sleeps := newTestServer(t, rateLimitedUpstream(t, "120", &attempts))

	if wait := requireRateLimited(t, progressTraversal(t, server)); wait != 2*time.Minute {
		t.Fatalf("retry after = %s, want 2m", wait)
	}
	// A long Retry-After defers the connection instead of waiting in place.
	if attempts.Load() != 1 || len(sleeps.recorded()) != 0 {
		t.Fatalf("attempts = %d, waits = %v", attempts.Load(), sleeps.recorded())
	}
}

func TestRateLimitUsesRetryAfterHTTPDate(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server, _ := newTestServer(t, rateLimitedUpstream(t, time.Now().Add(10*time.Minute).UTC().Format(http.TimeFormat), &attempts))

	wait := requireRateLimited(t, progressTraversal(t, server))
	// HTTP-dates have one-second resolution.
	if wait <= 10*time.Minute-5*time.Second || wait > 10*time.Minute {
		t.Fatalf("retry after = %s, want about 10m", wait)
	}
}

func TestRateLimitWithoutRetryAfterUsesFallback(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server, sleeps := newTestServer(t, rateLimitedUpstream(t, "", &attempts))

	if wait := requireRateLimited(t, progressTraversal(t, server)); wait != defaultRetryAfter {
		t.Fatalf("retry after = %s, want %s", wait, defaultRetryAfter)
	}
	if attempts.Load() != 1 || len(sleeps.recorded()) != 0 {
		t.Fatalf("attempts = %d, waits = %v, want one attempt", attempts.Load(), sleeps.recorded())
	}
}

func TestRateLimitShortRetryAfterRetriesInPlaceWithSameBody(t *testing.T) {
	t.Parallel()
	var bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if len(bodies) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()
	server, sleeps := newTestServer(t, upstream)
	// Keep the write limiter out of the way so only the Retry-After wait runs.
	server.writes = newCredentialLimiter(time.Nanosecond, 10)

	event := movieEvent("start-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"imdb": "tt123"})
	event.PositionSeconds, event.DurationSeconds = 60, 600
	got := statuses(t, applyEvents(t, server, event), event)
	if got["start-1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("status = %v", got["start-1"])
	}
	if len(bodies) != 2 || bodies[0] == "" || bodies[0] != bodies[1] {
		t.Fatalf("body not replayed identically: %#v", bodies)
	}
	if waits := sleeps.recorded(); !reflect.DeepEqual(waits, []time.Duration{time.Second}) {
		t.Fatalf("in-place waits = %v, want [1s]", waits)
	}
}

func TestRateLimitExhaustedInPlaceRetriesDeferForFallback(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server, sleeps := newTestServer(t, rateLimitedUpstream(t, "1", &attempts))

	if wait := requireRateLimited(t, progressTraversal(t, server)); wait != defaultRetryAfter {
		t.Fatalf("retry after = %s, want floored %s", wait, defaultRetryAfter)
	}
	if attempts.Load() != maxRetryAttempts+1 || len(sleeps.recorded()) != maxRetryAttempts {
		t.Fatalf("attempts = %d, waits = %v", attempts.Load(), sleeps.recorded())
	}
}

func TestRateLimitInPlaceRetryNeedsRoomBeforeTheDeadline(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server, sleeps := newTestServer(t, rateLimitedUpstream(t, "8", &attempts))
	client := &apiClient{server: server, app: appConfig{clientID: testClientID, clientSecret: testClientSecret}, token: testAccessToken}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _, f := client.do(ctx, http.MethodGet, "/sync/playback", nil, nil, nil)
	if wait := requireRateLimited(t, f); wait != 8*time.Second {
		t.Fatalf("retry after = %s, want 8s", wait)
	}
	if attempts.Load() != 1 || len(sleeps.recorded()) != 0 {
		t.Fatalf("attempts = %d, waits = %v; a wait past the deadline must defer", attempts.Load(), sleeps.recorded())
	}
}

func TestWriteLimiterPacesPerTokenAndLeavesReadsAlone(t *testing.T) {
	t.Parallel()
	var writes, reads atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			reads.Add(1)
			writeFixture(t, w, `[]`)
			return
		}
		writes.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	// One write per hour: a second write for one token can only proceed by
	// waiting, which the call's deadline refuses.
	server.writes = newCredentialLimiter(time.Hour, 1)
	event := movieEvent("start", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"imdb": "tt123"})
	event.DurationSeconds = 600
	apply := func(token string) *pluginv1.WatchSyncApplyEventsResponse {
		response, err := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
			Context: authContext(token), Events: []*pluginv1.WatchSyncEvent{event},
		})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	if response := apply("token-a"); response.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("first write = %v", response)
	}
	if wait := requireRateLimited(t, apply("token-a").GetFault()); wait != writeInterval {
		t.Fatalf("retry after = %s, want %s", wait, writeInterval)
	}
	if response := apply("token-b"); response.GetResults()[0].GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("token-b must not wait behind token-a: %v", response)
	}
	if writes.Load() != 2 {
		t.Fatalf("upstream saw %d writes, want 2", writes.Load())
	}
	for range 3 {
		if f := progressTraversal(t, server); f != nil {
			t.Fatalf("reads must not be paced by writes: %v", f)
		}
	}
	if reads.Load() != 3 {
		t.Fatalf("upstream saw %d reads, want 3", reads.Load())
	}
}

func TestTraktRequestsCarryTraktHeaders(t *testing.T) {
	t.Parallel()
	var header http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Clone()
		writeFixture(t, w, `[]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	if f := progressTraversal(t, server); f != nil {
		t.Fatal(f)
	}
	for key, want := range map[string]string{
		"Content-Type":      "application/json",
		"trakt-api-version": "2",
		"trakt-api-key":     testClientID,
		"Authorization":     "Bearer " + testAccessToken,
		// Trakt may block requests without an identifying User-Agent.
		"User-Agent": "silo-plugin-watchprovider-trakt/1.2.3",
	} {
		if got := header.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestUserAgentKeepsOnlyTokenCharacters(t *testing.T) {
	t.Parallel()
	for version, want := range map[string]string{
		"":                 "silo-plugin-watchprovider-trakt/dev",
		"v0.1.0":           "silo-plugin-watchprovider-trakt/0.1.0",
		"0.1.0-3-gabc1234": "silo-plugin-watchprovider-trakt/0.1.0-3-gabc1234",
		"0.1 (évil)\r\nX":  "silo-plugin-watchprovider-trakt/0.1vilX",
		" \t":              "silo-plugin-watchprovider-trakt/dev",
	} {
		if got := userAgent(version); got != want {
			t.Errorf("userAgent(%q) = %q, want %q", version, got, want)
		}
	}
}

// A token Trakt no longer accepts must reach the host as an invalid
// credential, so the profile owner is asked to reconnect. An app-level refusal
// must not, since reconnecting cannot fix the server's client ID.
func TestTraktCredentialRejections(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		header     map[string]string
		call       func(*Server) *pluginv1.WatchSyncFault
		wantCode   pluginv1.WatchSyncFaultCode
		wantDetail string
	}{
		{
			name:   "refresh token issued before Trakt's auth migration",
			status: http.StatusBadRequest,
			body:   `{"error":"invalid_grant","error_description":"session not found"}`,
			call: func(s *Server) *pluginv1.WatchSyncFault {
				response, _ := s.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext(testAccessToken)})
				return response.GetFault()
			},
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			wantDetail: "invalid_grant",
		},
		{
			name:       "revoked access token",
			status:     http.StatusUnauthorized,
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			wantDetail: "reconnect Trakt",
		},
		{
			name:       "unapproved app",
			status:     http.StatusForbidden,
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "client ID",
		},
		{
			name:   "rejected app secret on refresh",
			status: http.StatusUnauthorized,
			body:   `{"error":"invalid_client","error_description":"client secret ` + testClientSecret + ` is wrong"}`,
			call: func(s *Server) *pluginv1.WatchSyncFault {
				response, _ := s.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext(testAccessToken)})
				return response.GetFault()
			},
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "invalid_client",
		},
		{
			name:       "VIP item limit",
			status:     statusAccountLimitExceeded,
			header:     map[string]string{"X-Account-Limit": "100"},
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "limit of 100 items",
		},
		{
			name:       "VIP item limit without a usable header",
			status:     statusAccountLimitExceeded,
			header:     map[string]string{"X-Account-Limit": "<script>"},
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "reached an item limit",
		},
		{
			name:       "locked account",
			status:     http.StatusLocked,
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "locked",
		},
		{
			name:       "VIP feature",
			status:     http.StatusUpgradeRequired,
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			wantDetail: "needs Trakt VIP",
		},
		{
			name:       "outage",
			status:     http.StatusServiceUnavailable,
			body:       `<html>maintenance for ` + testAccessToken + `</html>`,
			call:       getAccountFault,
			wantCode:   pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY,
			wantDetail: "HTTP 503",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for key, value := range tc.header {
					w.Header().Set(key, value)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			f := tc.call(server)
			if f.GetCode() != tc.wantCode || !strings.Contains(f.GetSafeMessage(), tc.wantDetail) {
				t.Fatalf("fault = %s, want %v mentioning %q", faultText(f), tc.wantCode, tc.wantDetail)
			}
			// The safe message carries no tokens, secrets, or response text.
			requireNoSecrets(t, f.GetSafeMessage(), "session not found", "maintenance", "<script>", "is wrong")
		})
	}
}

func getAccountFault(s *Server) *pluginv1.WatchSyncFault {
	response, _ := s.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext(testAccessToken)})
	return response.GetFault()
}

func TestUnreachableTraktIsTemporary(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server, _ := newTestServer(t, upstream)
	upstream.Close()

	f := getAccountFault(server)
	if f.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("fault = %v, want TEMPORARY", f)
	}
	requireNoSecrets(t, f.GetSafeMessage(), upstream.URL)
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]struct {
		wait time.Duration
		ok   bool
	}{
		"":                              {0, false},
		"30":                            {30 * time.Second, true},
		"-1":                            {0, false},
		"soon":                          {0, false},
		"Mon, 04 May 2026 12:01:00 GMT": {time.Minute, true},
		"Mon, 04 May 2026 11:00:00 GMT": {0, true},
	} {
		wait, ok := parseRetryAfter(value, now)
		if wait != want.wait || ok != want.ok {
			t.Errorf("parseRetryAfter(%q) = %s, %v; want %s, %v", value, wait, ok, want.wait, want.ok)
		}
	}
}
