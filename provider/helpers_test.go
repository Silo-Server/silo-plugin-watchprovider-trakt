package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testClientID     = "client-id"
	testClientSecret = "client-secret-value"
	testAccessToken  = "access-token-value"
	testRefreshToken = "refresh-token-value"
)

// newTestServer returns a server that calls upstream and records, instead of
// sleeping through, every in-place wait.
func newTestServer(t *testing.T, upstream *httptest.Server) (*Server, *sleepRecorder) {
	t.Helper()
	server := NewServer(upstream.Client(), "1.2.3")
	server.baseURL = upstream.URL
	sleeps := &sleepRecorder{}
	server.sleep = sleeps.sleep
	return server, sleeps
}

type sleepRecorder struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (r *sleepRecorder) sleep(ctx context.Context, d time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waits = append(r.waits, d)
	return ctx.Err()
}

func (r *sleepRecorder) recorded() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.waits...)
}

func providerConfig() *pluginv1.WatchSyncProviderConfig {
	return &pluginv1.WatchSyncProviderConfig{
		Values:       map[string]string{configClientID: testClientID},
		SecretValues: map[string]string{configClientSecret: testClientSecret},
	}
}

// authContext is the context the host sends for a connection migrated from
// the built-in provider: tokens and expiry only.
func authContext(token string) *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId:   capabilityID,
		ProviderConfig: providerConfig(),
		Credentials: &pluginv1.WatchSyncCredentials{
			AccessToken:  token,
			RefreshToken: testRefreshToken,
			ExpiresAt:    timestamppb.New(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)),
		},
	}
}

func writeFixture(t *testing.T, w http.ResponseWriter, format string, args ...any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		t.Errorf("write fixture: %v", err)
	}
}

func decodeBody(t *testing.T, r *http.Request, out any) {
	t.Helper()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		t.Errorf("decode %s %s body: %v", r.Method, r.URL.Path, err)
	}
}

// traversalResult is what the host collects from one ListRemoteState
// traversal.
type traversalResult struct {
	items []*pluginv1.WatchSyncRemoteState
	calls int
	fault *pluginv1.WatchSyncFault
}

// traverse follows page tokens the way the host does: the cursor and kinds
// stay fixed, a repeated token or a changing snapshot mode is an error, and
// the first fault ends the traversal.
func traverse(t *testing.T, server *Server, kinds ...pluginv1.WatchSyncRemoteStateKind) traversalResult {
	t.Helper()
	var result traversalResult
	seen := map[string]bool{}
	pageToken := ""
	snapshot := (*bool)(nil)
	for result.calls < 10_000 {
		result.calls++
		response, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
			Context:    authContext(testAccessToken),
			PageToken:  pageToken,
			PageSize:   100,
			StateKinds: kinds,
		})
		if err != nil {
			t.Fatalf("ListRemoteState: %v", err)
		}
		if response.GetFault() != nil {
			result.fault = response.GetFault()
			result.items = nil
			return result
		}
		if snapshot != nil && *snapshot != response.GetCompleteSnapshot() {
			t.Fatal("snapshot mode changed during the traversal")
		}
		complete := response.GetCompleteSnapshot()
		snapshot = &complete
		if !complete {
			t.Fatal("traversal is not a complete snapshot")
		}
		if response.GetNextCursor() != "" {
			t.Fatalf("next cursor = %q, want none", response.GetNextCursor())
		}
		result.items = append(result.items, response.GetItems()...)
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			return result
		}
		if seen[pageToken] {
			t.Fatalf("page token repeated: %s", pageToken)
		}
		seen[pageToken] = true
	}
	t.Fatal("traversal did not end")
	return result
}

func applyEvents(t *testing.T, server *Server, events ...*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	response, err := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(testAccessToken),
		Events:  events,
	})
	if err != nil {
		t.Fatalf("ApplyEvents: %v", err)
	}
	return response
}

// statuses returns each result's status by event ID, failing on a
// connection-wide fault or a missing result.
func statuses(t *testing.T, response *pluginv1.WatchSyncApplyEventsResponse, events ...*pluginv1.WatchSyncEvent) map[string]pluginv1.WatchSyncApplyStatus {
	t.Helper()
	if response.GetFault() != nil {
		t.Fatalf("connection fault = %v", response.GetFault())
	}
	if len(response.GetResults()) != len(events) {
		t.Fatalf("results = %d, want %d", len(response.GetResults()), len(events))
	}
	out := map[string]pluginv1.WatchSyncApplyStatus{}
	for index, result := range response.GetResults() {
		if result.GetEventId() != events[index].GetEventId() {
			t.Fatalf("result %d is for %q, want %q", index, result.GetEventId(), events[index].GetEventId())
		}
		out[result.GetEventId()] = result.GetStatus()
	}
	return out
}

func movieEvent(id string, operation pluginv1.WatchSyncOperation, ids map[string]string) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:   id,
		Operation: operation,
		Media:     &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE, ExternalIds: ids},
	}
}

func seriesEvent(id string, operation pluginv1.WatchSyncOperation, ids map[string]string) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:   id,
		Operation: operation,
		Media:     &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, ExternalIds: ids},
	}
}

// watchedMovie is the MARK_WATCHED event the host builds for a local movie
// play: its provider item key is the host's own key for the play.
func watchedMovie(historyID, tmdbID string, watchedAt time.Time) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:         historyID,
		Operation:       pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		Origin:          pluginv1.WatchSyncOrigin_WATCH_SYNC_ORIGIN_RECONCILIATION,
		OccurredAt:      timestamppb.New(watchedAt),
		WatchHistoryId:  historyID,
		ProviderItemKey: "tmdb:" + tmdbID,
		Media: &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			ExternalIds: map[string]string{"tmdb": tmdbID},
		},
	}
}

func faultText(f *pluginv1.WatchSyncFault) string {
	return f.GetCode().String() + ": " + f.GetSafeMessage()
}

func requireNoSecrets(t *testing.T, message string, secrets ...string) {
	t.Helper()
	for _, secret := range append([]string{testAccessToken, testRefreshToken, testClientSecret}, secrets...) {
		if secret != "" && strings.Contains(message, secret) {
			t.Fatalf("message %q leaks %q", message, secret)
		}
	}
}
