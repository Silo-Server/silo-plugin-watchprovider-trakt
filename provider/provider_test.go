package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Every RPC refuses to call Trakt until an administrator configures the app,
// and says so without leaking what is configured.
func TestMissingAppConfigIsReportedOnEveryRPC(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	device := server.DeviceAuthorization()
	ctx := context.Background()

	for name, config := range map[string]*pluginv1.WatchSyncProviderConfig{
		"none":           nil,
		"no secret":      {Values: map[string]string{configClientID: testClientID}},
		"no client ID":   {SecretValues: map[string]string{configClientSecret: testClientSecret}},
		"blank client":   {Values: map[string]string{configClientID: "  "}, SecretValues: map[string]string{configClientSecret: testClientSecret}},
		"wrong key name": {Values: map[string]string{"client_id": testClientID}, SecretValues: map[string]string{"client_secret": testClientSecret}},
	} {
		auth := authContext(testAccessToken)
		auth.ProviderConfig = config
		start, _ := device.Start(ctx, &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{CapabilityId: capabilityID, ProviderConfig: config})
		poll, _ := device.Poll(ctx, &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{CapabilityId: capabilityID, ProviderConfig: config, ProviderState: []byte(`{"device_code":"x"}`)})
		refresh, _ := server.RefreshCredentials(ctx, &pluginv1.WatchSyncRefreshCredentialsRequest{Context: auth})
		account, _ := server.GetAccount(ctx, &pluginv1.WatchSyncGetAccountRequest{Context: auth})
		apply, _ := server.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{Context: auth, Events: []*pluginv1.WatchSyncEvent{watchedMovie("h1", "1", time.Now())}})
		list, _ := server.ListRemoteState(ctx, &pluginv1.WatchSyncListRemoteStateRequest{Context: auth})
		for rpc, f := range map[string]*pluginv1.WatchSyncFault{
			"Start": start.GetFault(), "Poll": poll.GetFault(), "RefreshCredentials": refresh.GetFault(),
			"GetAccount": account.GetFault(), "ApplyEvents": apply.GetFault(), "ListRemoteState": list.GetFault(),
		} {
			if f.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED || f.GetSafeMessage() != notConfiguredMessage {
				t.Errorf("%s %s: fault = %v", name, rpc, f)
			}
			requireNoSecrets(t, f.GetSafeMessage(), testClientID)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("upstream saw %d requests without app config", requests.Load())
	}
}

func TestUnknownCapabilityIsRejected(t *testing.T) {
	t.Parallel()
	auth := authContext(testAccessToken)
	auth.CapabilityId = "simkl"
	response, _ := NewServer(nil, "").GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: auth})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("fault = %v", response.GetFault())
	}
}

func TestGetAccountUsesTheSlugTheBuiltInProviderStored(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, wantID, wantUsername, wantDisplay string
	}{
		{"slug", `{"user":{"username":"Sean Rudford","name":"Sean","ids":{"slug":"sean-rudford"}}}`, "sean-rudford", "Sean Rudford", "Sean"},
		{"username fallback", `{"user":{"username":"sean","ids":{}}}`, "sean", "sean", "sean"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/users/settings" || r.Header.Get("Authorization") != "Bearer "+testAccessToken {
					t.Errorf("request = %s %s", r.URL.Path, r.Header.Get("Authorization"))
				}
				writeFixture(t, w, "%s", tc.body)
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			response, _ := server.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext(testAccessToken)})
			account := response.GetAccount()
			if response.GetFault() != nil || account.GetExternalSubject() != tc.wantID || account.GetUsername() != tc.wantUsername || account.GetDisplayName() != tc.wantDisplay {
				t.Fatalf("response = %v", response)
			}
		})
	}
}

func TestApplyEventsRejectsWhatTraktCannotAddress(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	noID := &pluginv1.WatchSyncEvent{Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED}
	noMedia := &pluginv1.WatchSyncEvent{EventId: "no-media", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE}
	unknown := movieEvent("unknown", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNSPECIFIED, map[string]string{"tmdb": "1"})
	seriesWatched := seriesEvent("series-watched", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED, map[string]string{"tvdb": "1"})
	seriesWatched.OccurredAt = timestamppb.Now()
	noTime := watchedMovie("no-time", "603", time.Now())
	noTime.OccurredAt = nil
	episodeFavorite := &pluginv1.WatchSyncEvent{EventId: "episode-favorite", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, ExternalIds: map[string]string{"tvdb": "1"}}}
	idlessFavorite := movieEvent("idless-favorite", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, nil)
	badRating := movieEvent("bad-rating", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, map[string]string{"tmdb": "1"})
	seriesScrobble := seriesEvent("series-scrobble", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"tvdb": "1"})
	events := []*pluginv1.WatchSyncEvent{noID, noMedia, unknown, seriesWatched, noTime, episodeFavorite, idlessFavorite, badRating, seriesScrobble}

	response := applyEvents(t, server, events...)
	if response.GetFault() != nil || len(response.GetResults()) != len(events) {
		t.Fatalf("response = %v", response)
	}
	for index, result := range response.GetResults() {
		if result.GetEventId() != events[index].GetEventId() || result.GetStatus() != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED ||
			result.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Errorf("result %d = %v", index, result)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("upstream saw %d requests for events it cannot take", requests.Load())
	}
}

// Consecutive events with one operation go in one request, in event order.
func TestApplyEventsBatchesRunsOfOneOperationInOrder(t *testing.T) {
	t.Parallel()
	var requests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		writeFixture(t, w, `{}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	server.writes = newCredentialLimiter(time.Nanosecond, 10)

	add := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE
	remove := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE
	events := []*pluginv1.WatchSyncEvent{
		movieEvent("a", add, map[string]string{"tmdb": "1"}),
		movieEvent("b", add, map[string]string{"tmdb": "2"}),
		movieEvent("c", remove, map[string]string{"tmdb": "1"}),
		movieEvent("d", add, map[string]string{"tmdb": "1"}),
	}
	for id, status := range statuses(t, applyEvents(t, server, events...), events...) {
		if status != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
			t.Errorf("%s = %v", id, status)
		}
	}
	want := "POST /sync/favorites,POST /sync/favorites/remove,POST /sync/favorites"
	if got := strings.Join(requests, ","); got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}

func TestApplyEventsDefersEventsPastTheTimeBox(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeFixture(t, w, `{}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	server.writes = newCredentialLimiter(time.Nanosecond, 10)
	// The clock jumps past the budget once the first request is sent.
	start := time.Now()
	server.now = func() time.Time {
		if requests.Load() > 0 {
			return start.Add(time.Hour)
		}
		return start
	}

	first := movieEvent("first", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, map[string]string{"tmdb": "1"})
	second := movieEvent("second", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE, map[string]string{"tmdb": "2"})
	response := applyEvents(t, server, first, second)
	got := statuses(t, response, first, second)
	if got["first"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || got["second"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
		t.Fatalf("statuses = %v", got)
	}
	if f := response.GetResults()[1].GetFault(); f.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY {
		t.Fatalf("deferred fault = %v", f)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}
}

func TestWindowEndsBeforeTheHostDeadline(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "")
	now := time.Now()
	server.now = func() time.Time { return now }

	parent, cancel := context.WithDeadline(context.Background(), now.Add(2*time.Minute))
	defer cancel()
	ctx, stop, stopAt := server.window(parent)
	defer stop()
	deadline, _ := ctx.Deadline()
	if !deadline.Equal(now.Add(2*time.Minute - syncDeadlineMargin)) {
		t.Fatalf("request deadline = %s after now", deadline.Sub(now))
	}
	if !stopAt.Equal(now.Add(syncBudget)) || stopAt.Add(defaultRequestTimeout).After(deadline) {
		t.Fatalf("stop at %s after now, deadline %s after now", stopAt.Sub(now), deadline.Sub(now))
	}

	short, cancelShort := context.WithDeadline(context.Background(), now.Add(time.Minute))
	defer cancelShort()
	_, stopShort, stopAt := server.window(short)
	defer stopShort()
	if want := now.Add(time.Minute - syncDeadlineMargin - defaultRequestTimeout); !stopAt.Equal(want) {
		t.Fatalf("stop at %s after now, want %s", stopAt.Sub(now), want.Sub(now))
	}
}
