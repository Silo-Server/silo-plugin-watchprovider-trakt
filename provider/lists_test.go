package provider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var listOperations = []struct {
	name      string
	operation pluginv1.WatchSyncOperation
	path      string
	remove    bool
}{
	{"add favorite", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, "/sync/favorites", false},
	{"remove favorite", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE, "/sync/favorites/remove", true},
	{"add to watchlist", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, "/sync/watchlist", false},
	{"remove from watchlist", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST, "/sync/watchlist/remove", true},
}

// runList sends events to a fake Trakt that answers path with the given
// not_found body, and returns the decoded request body and the statuses.
func runList(t *testing.T, path, notFound string, events ...*pluginv1.WatchSyncEvent) (traktIDList, map[string]pluginv1.WatchSyncApplyStatus) {
	t.Helper()
	var body traktIDList
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != path {
			t.Errorf("request = %s %s, want POST %s", r.Method, r.URL.Path, path)
		}
		decodeBody(t, r, &body)
		writeFixture(t, w, `{"not_found":%s}`, notFound)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	got := statuses(t, applyEvents(t, server, events...), events...)
	if requests > 1 {
		t.Fatalf("requests = %d, want one batch", requests)
	}
	return body, got
}

func TestListEventsSendMovieAndShowPayload(t *testing.T) {
	t.Parallel()
	for _, op := range listOperations {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			movie := movieEvent("movie-1", op.operation, map[string]string{"imdb": "tt123"})
			show := seriesEvent("show-1", op.operation, map[string]string{"tvdb": "789"})
			body, got := runList(t, op.path, `{"movies":[],"shows":[]}`, movie, show)
			want := traktIDList{Movies: []traktIDItem{{IDs: traktIDs{IMDb: "tt123"}}}, Shows: []traktIDItem{{IDs: traktIDs{TVDB: 789}}}}
			if !reflect.DeepEqual(body, want) {
				t.Fatalf("body = %+v, want %+v", body, want)
			}
			for id, status := range got {
				if status != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
					t.Errorf("%s = %v", id, status)
				}
			}
		})
	}
}

// Items without external ids are sent by the id their provider item key
// encodes, including a bare Trakt id from an earlier import.
func TestListEventsFallBackToTheProviderItemKey(t *testing.T) {
	t.Parallel()
	show := seriesEvent("show-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE, nil)
	show.ProviderItemKey = "tvdb:789"
	movie := movieEvent("movie-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE, nil)
	movie.ProviderItemKey = "trakt:5"
	body, got := runList(t, "/sync/favorites/remove", `{"movies":[],"shows":[]}`, show, movie)
	if len(body.Shows) != 1 || body.Shows[0].IDs != (traktIDs{TVDB: 789}) || len(body.Movies) != 1 || body.Movies[0].IDs != (traktIDs{Trakt: 5}) {
		t.Fatalf("body = %+v", body)
	}
	if got["show-1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || got["movie-1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("statuses = %v", got)
	}
}

func TestListNotFoundMatchesAShowByAnySharedID(t *testing.T) {
	t.Parallel()
	for _, op := range listOperations {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			// Silo keys the show by IMDb; Trakt echoes it back by TVDB alone.
			show := seriesEvent("show-1", op.operation, map[string]string{"imdb": "tt0903747", "tvdb": "81189"})
			show.ProviderItemKey = "imdb:tt0903747"
			_, got := runList(t, op.path, `{"movies":[],"shows":[{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":81189}}]}`, show)
			// A title Trakt does not know cannot be added, and has nothing to remove.
			want := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
			if op.remove {
				want = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
			}
			if got["show-1"] != want {
				t.Fatalf("status = %v, want %v", got["show-1"], want)
			}
		})
	}
}

func TestListNotFoundInAMixedBatchReportsOnlyTheMissingItem(t *testing.T) {
	t.Parallel()
	add := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE
	found := movieEvent("movie-found", add, map[string]string{"imdb": "tt0133093", "tmdb": "603"})
	missing := movieEvent("movie-missing", add, map[string]string{"tmdb": "550"})
	// Same TMDB number as the missing movie, but TMDB numbers shows
	// separately, so the movie echo must not match it.
	showFound := seriesEvent("show-found", add, map[string]string{"tmdb": "550", "tvdb": "81189"})
	// Documented limitation: an echo sharing no id with the item cannot be
	// attributed, so the item counts as applied.
	unattributed := movieEvent("movie-unattributed", add, map[string]string{"imdb": "tt0111161"})
	body, got := runList(t, "/sync/favorites", `{"movies":[{"ids":{"tmdb":550}},{"ids":{"trakt":12601}}],"shows":[]}`, found, missing, showFound, unattributed)
	if len(body.Movies) != 3 || len(body.Shows) != 1 {
		t.Fatalf("body = %+v", body)
	}
	want := map[string]pluginv1.WatchSyncApplyStatus{
		"movie-found":        pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		"movie-missing":      pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED,
		"show-found":         pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		"movie-unattributed": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}

func TestListEventsWithoutAnyIDAreRejectedUnsent(t *testing.T) {
	t.Parallel()
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	event := movieEvent("movie-no-ids", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, nil)
	event.ProviderItemKey = "episode:s1:e2"
	if got := statuses(t, applyEvents(t, server, event), event); got["movie-no-ids"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED || requests != 0 {
		t.Fatalf("status = %v, requests = %d", got["movie-no-ids"], requests)
	}
}

// An account limit applies to the whole request, as the built-in provider
// failed the whole batch; the host records the message on every item.
func TestListAccountLimitIsAConnectionFault(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Account-Limit", "100")
		w.WriteHeader(statusAccountLimitExceeded)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	event := movieEvent("movie-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, map[string]string{"tmdb": "1"})
	response := applyEvents(t, server, event)
	if f := response.GetFault(); f.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED ||
		f.GetSafeMessage() != "Trakt request POST /sync/watchlist failed (HTTP 420): the Trakt account has reached its limit of 100 items; Trakt VIP raises the limit" {
		t.Fatalf("fault = %v", f)
	}
}

func TestRatingEventsSendRatingsAndMapNotFound(t *testing.T) {
	t.Parallel()
	ratedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	var got traktRatingsPayload
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sync/ratings" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		decodeBody(t, r, &got)
		w.WriteHeader(http.StatusCreated)
		// Trakt echoes the missing show by TVDB id only.
		writeFixture(t, w, `{"added":{"movies":1,"shows":0},"not_found":{"movies":[],"shows":[{"rating":8,"ids":{"tvdb":79126}}]}}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	set := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING
	movie := movieEvent("m1", set, map[string]string{"imdb": "tt0113277"})
	movie.Rating, movie.ProviderItemKey, movie.OccurredAt = 6, "imdb:tt0113277", timestamppb.New(ratedAt)
	show := seriesEvent("s1", set, map[string]string{"imdb": "tt0306414", "tvdb": "79126"})
	show.Rating, show.ProviderItemKey = 8, "imdb:tt0306414"
	result := statuses(t, applyEvents(t, server, movie, show), movie, show)

	if len(got.Movies) != 1 || got.Movies[0].Rating != 6 || got.Movies[0].IDs.IMDb != "tt0113277" || got.Movies[0].RatedAt == nil || !got.Movies[0].RatedAt.Equal(ratedAt) {
		t.Fatalf("movie payload = %#v", got.Movies)
	}
	if len(got.Shows) != 1 || got.Shows[0].Rating != 8 || got.Shows[0].RatedAt != nil {
		t.Fatalf("show payload = %#v", got.Shows)
	}
	if result["m1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || result["s1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		t.Fatalf("statuses = %v, want the movie applied and the show rejected", result)
	}
}

// Removing a rating sends ids only, and removing one Trakt does not hold
// converges without a fault.
func TestRatingRemovalSendsIDsOnlyAndConverges(t *testing.T) {
	t.Parallel()
	var got map[string][]map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/sync/ratings/remove" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		decodeBody(t, r, &got)
		writeFixture(t, w, `{"deleted":{"movies":1},"not_found":{"movies":[{"ids":{"tmdb":550}}],"shows":[]}}`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	remove := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	rated := movieEvent("m1", remove, nil)
	rated.ProviderItemKey = "tmdb:949"
	unknown := movieEvent("m2", remove, map[string]string{"tmdb": "550"})
	result := statuses(t, applyEvents(t, server, rated, unknown), rated, unknown)
	if len(got["movies"]) != 2 || got["movies"][0]["rating"] != nil {
		t.Fatalf("remove payload = %#v, want ids without a rating", got)
	}
	if result["m1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || result["m2"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("statuses = %v", result)
	}
}

func TestRatingEventsForEpisodesAreRejected(t *testing.T) {
	t.Parallel()
	server := NewServer(nil, "")
	event := &pluginv1.WatchSyncEvent{EventId: "e1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, Rating: 7,
		Media: &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, ExternalIds: map[string]string{"tvdb": "1"}}}
	if got := statuses(t, applyEvents(t, server, event), event); got["e1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED {
		t.Fatalf("status = %v", got["e1"])
	}
}

func TestScrobblePayloadNamesTheItemLikeTheBuiltInProvider(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 4, 23, 30, 0, 0, time.FixedZone("EDT", -4*60*60))
	episodeWith := func(own map[string]string) *pluginv1.WatchSyncEvent {
		return &pluginv1.WatchSyncEvent{
			PositionSeconds: 300, DurationSeconds: 1200,
			Media: &pluginv1.WatchSyncMedia{
				MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE, ExternalIds: own,
				SeriesExternalIds: map[string]string{"tvdb": "79126"}, SeasonNumber: 1, EpisodeNumber: 2,
			},
		}
	}
	movie := movieEvent("m", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"imdb": "tt1", "tmdb": "2"})
	for name, tc := range map[string]struct {
		event *pluginv1.WatchSyncEvent
		want  string
	}{
		"movie":             {movie, `{"app_date":"2026-05-05","app_version":"Silo","movie":{"ids":{"trakt":0,"slug":"","imdb":"tt1","tmdb":2,"tvdb":0}},"progress":0}`},
		"episode by ids":    {episodeWith(map[string]string{"tvdb": "349232"}), `{"app_date":"2026-05-05","app_version":"Silo","episode":{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":349232}},"progress":25}`},
		"episode by number": {episodeWith(nil), `{"app_date":"2026-05-05","app_version":"Silo","episode":{"number":2,"season":1},"progress":25,"show":{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":79126}}}`},
	} {
		encoded, _ := json.Marshal(scrobblePayload(tc.event, now))
		if string(encoded) != tc.want {
			t.Errorf("%s: payload = %s\nwant %s", name, encoded, tc.want)
		}
	}
}

func TestScrobbleMapsTraktAnswers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		want   pluginv1.WatchSyncApplyStatus
	}{
		{"recorded", http.StatusCreated, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED},
		// Trakt refuses to scrobble the same item twice in a row.
		{"already scrobbled", http.StatusConflict, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE},
		{"unknown item", http.StatusNotFound, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED},
		{"outage", http.StatusServiceUnavailable, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var paths []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"watched_at":"2026-05-04T12:00:00.000Z","expires_at":"2026-05-04T13:00:00.000Z"}`))
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			event := movieEvent("scrobble:stop:session-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, map[string]string{"imdb": "tt1"})
			event.PositionSeconds, event.DurationSeconds, event.Completed = 5400, 6000, true
			if got := statuses(t, applyEvents(t, server, event), event); got[event.GetEventId()] != tc.want {
				t.Fatalf("status = %v, want %v", got[event.GetEventId()], tc.want)
			}
			if !reflect.DeepEqual(paths, []string{"/scrobble/stop"}) {
				t.Fatalf("paths = %v", paths)
			}
		})
	}
}
