package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeHistory is a Trakt history: GET /sync/history lists the plays inside
// start_at..end_at one per page, and POST /sync/history stores every play it
// is sent, a repeat included, at minute precision.
type fakeHistory struct {
	mu      sync.Mutex
	t       *testing.T
	plays   []fakePlay
	gets    []string
	posts   []traktHistoryPayload
	removes []traktHistoryRemovePayload
	nextID  int64
}

type fakePlay struct {
	id        int64
	kind      string
	ids       traktIDs
	show      traktIDs
	season    int
	number    int
	watchedAt time.Time
}

func (f *fakeHistory) add(play fakePlay) {
	f.nextID++
	play.id = f.nextID
	play.watchedAt = play.watchedAt.UTC().Truncate(time.Minute)
	f.plays = append(f.plays, play)
}

func (f *fakeHistory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.t
	switch {
	case r.Method == http.MethodGet && r.URL.Path == historyPath:
		query := r.URL.Query()
		f.gets = append(f.gets, query.Get("start_at")+".."+query.Get("end_at")+"#"+query.Get("page"))
		from, errFrom := time.Parse(traktTimeFormat, query.Get("start_at"))
		to, errTo := time.Parse(traktTimeFormat, query.Get("end_at"))
		if errFrom != nil || errTo != nil || query.Get("limit") != "250" {
			t.Errorf("history query = %s", r.URL.RawQuery)
		}
		var rows []map[string]any
		for _, play := range f.plays {
			if play.watchedAt.Before(from) || play.watchedAt.After(to) {
				continue
			}
			row := map[string]any{"id": play.id, "type": play.kind, "watched_at": play.watchedAt.Format(traktTimeFormat)}
			if play.kind == "movie" {
				row["movie"] = map[string]any{"ids": play.ids}
			} else {
				row["show"] = map[string]any{"ids": play.show}
				row["episode"] = map[string]any{"season": play.season, "number": play.number, "ids": play.ids}
			}
			rows = append(rows, row)
		}
		w.Header().Set("X-Pagination-Page-Count", "1")
		w.Header().Set("X-Pagination-Item-Count", fmt.Sprint(len(rows)))
		if rows == nil {
			rows = []map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(rows)
	case r.Method == http.MethodPost && r.URL.Path == historyPath:
		var payload traktHistoryPayload
		decodeBody(t, r, &payload)
		f.posts = append(f.posts, payload)
		for _, movie := range payload.Movies {
			at, _ := time.Parse(time.RFC3339, movie.WatchedAt)
			f.add(fakePlay{kind: "movie", ids: movie.IDs, watchedAt: at})
		}
		for _, episode := range payload.Episodes {
			at, _ := time.Parse(time.RFC3339, episode.WatchedAt)
			f.add(fakePlay{kind: "episode", ids: episode.IDs, show: traktIDs{TVDB: 79126}, season: 1, number: 2, watchedAt: at})
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"added":{}}`))
	case r.Method == http.MethodPost && r.URL.Path == historyPath+"/remove":
		var payload traktHistoryRemovePayload
		decodeBody(t, r, &payload)
		f.removes = append(f.removes, payload)
		_, _ = w.Write([]byte(`{"deleted":{}}`))
	default:
		t.Errorf("unexpected request %s %s", r.Method, r.URL)
		http.NotFound(w, r)
	}
}

func newFakeHistory(t *testing.T) (*fakeHistory, *Server) {
	t.Helper()
	fake := &fakeHistory{t: t}
	upstream := httptest.NewServer(fake)
	t.Cleanup(upstream.Close)
	server, _ := newTestServer(t, upstream)
	server.writes = newCredentialLimiter(time.Nanosecond, 100)
	return fake, server
}

func TestApplyWatchedSkipsPlaysTraktAlreadyHas(t *testing.T) {
	t.Parallel()
	fake, server := newFakeHistory(t)
	// Trakt keeps watched_at to the minute.
	fake.add(fakePlay{kind: "movie", ids: traktIDs{TMDB: 603, IMDb: "tt0133093"}, watchedAt: time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)})

	present := watchedMovie("history-1", "603", time.Date(2026, 5, 4, 12, 0, 40, 0, time.UTC))
	missing := watchedMovie("history-2", "604", time.Date(2026, 5, 4, 13, 0, 5, 0, time.UTC))
	got := statuses(t, applyEvents(t, server, present, missing), present, missing)
	if got["history-1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE || got["history-2"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
		t.Fatalf("statuses = %v", got)
	}
	// One read of the batch's window, then one write of the new play only.
	if want := []string{"2026-05-04T11:59:00.000Z..2026-05-04T13:02:00.000Z#1"}; !reflect.DeepEqual(fake.gets, want) {
		t.Fatalf("history reads = %v, want %v", fake.gets, want)
	}
	if len(fake.posts) != 1 || len(fake.posts[0].Movies) != 1 || fake.posts[0].Movies[0].IDs.TMDB != 604 ||
		fake.posts[0].Movies[0].WatchedAt != "2026-05-04T13:00:05Z" {
		t.Fatalf("writes = %+v", fake.posts)
	}
}

// ApplyEvents is at-least-once: a redelivered play must not add a second
// play, because Trakt stores duplicates.
func TestApplyWatchedIsIdempotentAcrossAtLeastOnceDelivery(t *testing.T) {
	t.Parallel()
	fake, server := newFakeHistory(t)
	at := time.Date(2026, 8, 5, 12, 34, 56, 0, time.UTC)
	movie := watchedMovie("history-movie", "603", at)
	episode := &pluginv1.WatchSyncEvent{
		EventId:         "history-episode",
		Operation:       pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		OccurredAt:      timestamppb.New(at.Add(time.Hour)),
		ProviderItemKey: "tvdb:349232",
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			ExternalIds:       map[string]string{"tvdb": "349232"},
			SeriesExternalIds: map[string]string{"tvdb": "79126"},
			SeasonNumber:      1, EpisodeNumber: 2,
		},
	}

	first := statuses(t, applyEvents(t, server, movie, episode), movie, episode)
	again := statuses(t, applyEvents(t, server, movie, episode), movie, episode)
	for id := range first {
		if first[id] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED || again[id] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
			t.Fatalf("%s: first %v, again %v", id, first[id], again[id])
		}
	}
	if len(fake.plays) != 2 || len(fake.posts) != 1 {
		t.Fatalf("plays = %d, writes = %d; want one play each from one write", len(fake.plays), len(fake.posts))
	}
}

// Silo keys an episode it knows only by show and number differently from the
// episode ids Trakt's history returns; the play must still be found.
func TestApplyWatchedFindsAnEpisodeKnownOnlyByShowAndNumber(t *testing.T) {
	t.Parallel()
	fake, server := newFakeHistory(t)
	at := time.Date(2026, 8, 5, 20, 0, 0, 0, time.UTC)
	fake.add(fakePlay{kind: "episode", ids: traktIDs{Trakt: 9, TVDB: 349232}, show: traktIDs{Trakt: 2, TVDB: 79126, TMDB: 1438}, season: 1, number: 2, watchedAt: at})
	event := &pluginv1.WatchSyncEvent{
		EventId:         "history-episode",
		Operation:       pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		OccurredAt:      timestamppb.New(at.Add(30 * time.Second)),
		ProviderItemKey: "show:tvdb:79126:s1:e2",
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeriesExternalIds: map[string]string{"tvdb": "79126", "tmdb": "1438"},
			SeasonNumber:      1, EpisodeNumber: 2,
		},
	}
	if got := statuses(t, applyEvents(t, server, event), event); got["history-episode"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE {
		t.Fatalf("status = %v", got["history-episode"])
	}
	if len(fake.posts) != 0 {
		t.Fatalf("writes = %+v", fake.posts)
	}
}

// Each remote play stands for one local play, as in the built-in provider's
// export reconciliation; a repeated event ID is still one play.
func TestApplyWatchedCountsPlaysAndIgnoresRepeatedEventIDs(t *testing.T) {
	t.Parallel()
	fake, server := newFakeHistory(t)
	minute := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	fake.add(fakePlay{kind: "movie", ids: traktIDs{TMDB: 603}, watchedAt: minute})

	first := watchedMovie("history-0", "603", minute.Add(10*time.Second))
	second := watchedMovie("history-1", "603", minute.Add(30*time.Second))
	repeat := watchedMovie("history-1", "603", minute.Add(30*time.Second))
	response := applyEvents(t, server, first, second, repeat)
	if response.GetFault() != nil || len(response.GetResults()) != 3 {
		t.Fatalf("response = %v", response)
	}
	want := []pluginv1.WatchSyncApplyStatus{
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
	}
	for index, result := range response.GetResults() {
		if result.GetStatus() != want[index] {
			t.Errorf("result %d = %v, want %v", index, result, want[index])
		}
	}
	if len(fake.posts) != 1 || len(fake.posts[0].Movies) != 1 {
		t.Fatalf("writes = %+v, want the one new play", fake.posts)
	}
}

// TestApplyWatchedDoesNotResendPlayFromLaterHistoryPage reads a paged history
// window that changes before settling. Only a play absent from a stable
// history may be sent.
func TestApplyWatchedDoesNotResendPlayFromLaterHistoryPage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		change    string
		wantError bool
	}{
		{name: "stable history"},
		{name: "count changes during first pass", change: "first pass"},
		{name: "count changes during verification pass", change: "verification pass"},
		{name: "rows change between passes", change: "between passes"},
		{name: "retry still requires matching passes", change: "retried rows"},
		{name: "history never settles", change: "every pass", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sent traktHistoryPayload
			var posts, pass, gets atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == historyPath:
					page := r.URL.Query().Get("page")
					gets.Add(1)
					if page == "1" {
						pass.Add(1)
					}
					if gets.Load() > 12 || (tc.wantError && gets.Load() > 6) {
						t.Error("history kept retrying instead of failing")
						http.Error(w, "too many reads", http.StatusServiceUnavailable)
						return
					}
					countChanged := false
					switch tc.change {
					case "every pass":
						countChanged = true
					case "first pass", "retried rows":
						countChanged = pass.Load() == 1
					case "verification pass":
						countChanged = pass.Load() == 2
					}
					count := "2"
					if page == "1" && countChanged {
						count = "3"
					}
					w.Header().Set("X-Pagination-Item-Count", count)
					w.Header().Set("X-Pagination-Page-Count", count)
					w.Header().Set("X-Pagination-Limit", "1")
					switch page {
					case "1":
						// An equal-count replacement must discard the first complete pass.
						tmdb := 101
						if (tc.change == "between passes" && pass.Load() == 1) || (tc.change == "retried rows" && pass.Load() == 2) {
							tmdb = 999
						}
						writeFixture(t, w, `[{"id":1,"type":"movie","watched_at":"2026-05-01T12:00:00.000Z","movie":{"ids":{"tmdb":%d}}}]`, tmdb)
					case "2":
						writeFixture(t, w, `[{"id":2,"type":"movie","watched_at":"2026-05-02T12:00:00.000Z","movie":{"ids":{"tmdb":102}}}]`)
					default:
						t.Errorf("unexpected history page %q", page)
						http.Error(w, "unexpected page", http.StatusInternalServerError)
					}
				case r.Method == http.MethodPost && r.URL.Path == historyPath:
					posts.Add(1)
					decodeBody(t, r, &sent)
					w.WriteHeader(http.StatusCreated)
					writeFixture(t, w, `{}`)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			events := []*pluginv1.WatchSyncEvent{
				watchedMovie("history-1", "101", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)), // remote page 1
				watchedMovie("history-2", "102", time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)), // remote page 2
				watchedMovie("history-3", "103", time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)), // not on Trakt
			}
			got := statuses(t, applyEvents(t, server, events...), events...)
			if tc.wantError {
				for id, status := range got {
					if status != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY {
						t.Errorf("%s = %v, want RETRY", id, status)
					}
				}
				if posts.Load() != 0 {
					t.Fatalf("posts = %d, want none", posts.Load())
				}
				return
			}
			want := map[string]pluginv1.WatchSyncApplyStatus{
				"history-1": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
				"history-2": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
				"history-3": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("statuses = %v, want %v", got, want)
			}
			if posts.Load() != 1 || len(sent.Movies) != 1 || sent.Movies[0].IDs.TMDB != 103 || len(sent.Episodes) != 0 || len(sent.Shows) != 0 {
				t.Fatalf("posts = %d, sent = %+v; want one write of tmdb 103 only", posts.Load(), sent)
			}
		})
	}
}

// A batch spread over years would read the whole history in one window, so
// a window with too many pages is split until each part is small.
func TestApplyWatchedSplitsAWindowWithTooManyPages(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var windows []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			return
		}
		from, _ := time.Parse(traktTimeFormat, r.URL.Query().Get("start_at"))
		to, _ := time.Parse(traktTimeFormat, r.URL.Query().Get("end_at"))
		mu.Lock()
		windows = append(windows, from.Format("2006-01-02")+".."+to.Format("2006-01-02"))
		mu.Unlock()
		if to.Sub(from) > 24*time.Hour {
			w.Header().Set("X-Pagination-Page-Count", "40")
			writeFixture(t, w, `[{"id":7,"type":"movie","watched_at":"2021-01-01T00:00:00.000Z","movie":{"ids":{"tmdb":7}}}]`)
			return
		}
		w.Header().Set("X-Pagination-Page-Count", "1")
		writeFixture(t, w, `[{"id":1,"type":"movie","watched_at":"2020-01-01T12:00:00.000Z","movie":{"ids":{"tmdb":1}}}]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	events := []*pluginv1.WatchSyncEvent{
		watchedMovie("old", "1", time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)),
		watchedMovie("middle", "2", time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)),
		watchedMovie("new", "3", time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)),
	}
	got := statuses(t, applyEvents(t, server, events...), events...)
	want := map[string]pluginv1.WatchSyncApplyStatus{
		"old":    pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE,
		"middle": pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
		"new":    pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
	sort.Strings(windows)
	wantWindows := []string{"2020-01-01..2020-01-01", "2020-01-01..2026-05-04", "2023-06-01..2023-06-01", "2023-06-01..2026-05-04", "2026-05-04..2026-05-04"}
	sort.Strings(wantWindows)
	if !reflect.DeepEqual(windows, wantWindows) {
		t.Fatalf("windows = %v, want %v", windows, wantWindows)
	}
}

func TestApplyWatchedDefersPlaysPastTheTimeBox(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	start := time.Now()
	calls := 0
	// The first reading of the clock opens the call; every later one is past
	// its budget.
	server.now = func() time.Time {
		calls++
		if calls > 2 {
			return start.Add(time.Hour)
		}
		return start
	}

	event := watchedMovie("history-1", "603", start)
	response := applyEvents(t, server, event)
	if got := statuses(t, response, event); got["history-1"] != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY ||
		response.GetResults()[0].GetFault().GetSafeMessage() != timeLimitMessage {
		t.Fatalf("response = %v", response)
	}
	if requests.Load() != 0 {
		t.Fatalf("requests = %d, want none", requests.Load())
	}
}

func TestApplyWatchedMapsUpstreamFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		get, post  int
		header     map[string]string
		wantFault  pluginv1.WatchSyncFaultCode
		wantStatus pluginv1.WatchSyncApplyStatus
	}{
		{name: "revoked token on read", get: http.StatusUnauthorized, wantFault: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL},
		{name: "revoked token on write", get: http.StatusOK, post: http.StatusUnauthorized, wantFault: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL},
		{name: "VIP limit on write", get: http.StatusOK, post: statusAccountLimitExceeded, header: map[string]string{"X-Account-Limit": "100"}, wantFault: pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED},
		{name: "outage on read", get: http.StatusServiceUnavailable, wantStatus: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
		{name: "outage on write", get: http.StatusOK, post: http.StatusBadGateway, wantStatus: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
		// Like the built-in provider's failed batch, a refused write stays
		// retryable instead of marking the plays unknown to Trakt.
		{name: "refused write", get: http.StatusOK, post: http.StatusUnprocessableEntity, wantStatus: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
		{name: "refused read", get: http.StatusBadRequest, wantStatus: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
		{name: "missing read", get: http.StatusNotFound, wantStatus: pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for key, value := range tc.header {
					w.Header().Set(key, value)
				}
				status := tc.get
				if r.Method == http.MethodPost {
					status = tc.post
				}
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`[]`))
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			event := watchedMovie("history-1", "603", time.Now())
			response := applyEvents(t, server, event)
			if tc.wantFault != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_UNSPECIFIED {
				if response.GetFault().GetCode() != tc.wantFault || len(response.GetResults()) != 0 {
					t.Fatalf("response = %v, want connection fault %v", response, tc.wantFault)
				}
				requireNoSecrets(t, response.GetFault().GetSafeMessage())
				return
			}
			if got := statuses(t, response, event); got["history-1"] != tc.wantStatus {
				t.Fatalf("status = %v, want %v", got["history-1"], tc.wantStatus)
			}
		})
	}
}

func TestApplyUnwatchedSendsTraktRemovePayload(t *testing.T) {
	t.Parallel()
	fake, server := newFakeHistory(t)
	movie := movieEvent("unwatched:history-1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED, map[string]string{"imdb": "tt123", "tmdb": "456"})
	episode := &pluginv1.WatchSyncEvent{
		EventId:         "unwatched:history-2",
		Operation:       pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED,
		ProviderItemKey: "show:tvdb:789:s0:e2",
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeriesExternalIds: map[string]string{"tvdb": "789"},
			SeasonNumber:      0, EpisodeNumber: 2,
		},
	}
	got := statuses(t, applyEvents(t, server, movie, episode), movie, episode)
	for id, status := range got {
		if status != pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED {
			t.Errorf("%s = %v", id, status)
		}
	}
	if len(fake.removes) != 1 {
		t.Fatalf("removes = %+v", fake.removes)
	}
	body, _ := json.Marshal(fake.removes[0])
	// The episode carries only a series id, so it lands in the nested shows[]
	// structure; nothing carries watched_at, so every play is removed.
	want := `{"movies":[{"ids":{"trakt":0,"slug":"","imdb":"tt123","tmdb":456,"tvdb":0}}],"shows":[{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":789},"seasons":[{"number":0,"episodes":[{"number":2}]}]}]}`
	if string(body) != want {
		t.Fatalf("remove payload = %s\nwant %s", body, want)
	}
}

func episodePlay(own, series traktIDs, season, number int, at time.Time) localPlay {
	return localPlay{kind: kindEpisode, own: own, series: series, season: season, episode: number, watchedAt: at}
}

func TestHistoryPayloadsIncludeTVDBOnlyMovieIDs(t *testing.T) {
	t.Parallel()
	play := localPlay{kind: kindMovie, own: traktIDs{TVDB: 12345}, watchedAt: time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)}
	if payload := historyPayload([]localPlay{play}); len(payload.Movies) != 1 || payload.Movies[0].IDs.TVDB != 12345 {
		t.Fatalf("add payload movies = %#v", payload.Movies)
	}
	if payload := historyRemovePayload([]localPlay{play}); len(payload.Movies) != 1 || payload.Movies[0].IDs.TVDB != 12345 {
		t.Fatalf("remove payload movies = %#v", payload.Movies)
	}
}

func TestHistoryEpisodeWithoutOwnIDsUsesNestedShowFallback(t *testing.T) {
	t.Parallel()
	play := episodePlay(traktIDs{}, traktIDs{TMDB: 999}, 2, 5, time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC))

	add, _ := json.Marshal(historyPayload([]localPlay{play}))
	if want := `{"shows":[{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":999,"tvdb":0},"seasons":[{"number":2,"episodes":[{"number":5,"watched_at":"2026-05-04T12:00:00Z"}]}]}]}`; string(add) != want {
		t.Fatalf("add payload = %s\nwant %s", add, want)
	}
	remove, _ := json.Marshal(historyRemovePayload([]localPlay{play}))
	if want := `{"shows":[{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":999,"tvdb":0},"seasons":[{"number":2,"episodes":[{"number":5}]}]}]}`; string(remove) != want {
		t.Fatalf("remove payload = %s\nwant %s", remove, want)
	}
}

func TestHistoryNestedFallbackMergesEpisodesBySeason(t *testing.T) {
	t.Parallel()
	plays := []localPlay{
		episodePlay(traktIDs{}, traktIDs{TMDB: 999}, 1, 3, time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)),
		episodePlay(traktIDs{}, traktIDs{TMDB: 999}, 1, 4, time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)),
		episodePlay(traktIDs{}, traktIDs{TMDB: 999}, 2, 1, time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)),
	}
	add := historyPayload(plays)
	if len(add.Shows) != 1 || len(add.Shows[0].Seasons) != 2 || len(add.Shows[0].Seasons[0].Episodes) != 2 ||
		add.Shows[0].Seasons[0].Episodes[0].Number != 3 || add.Shows[0].Seasons[0].Episodes[1].Number != 4 {
		t.Fatalf("add payload = %+v", add.Shows)
	}
	remove := historyRemovePayload(plays)
	if len(remove.Shows) != 1 || len(remove.Shows[0].Seasons) != 2 || len(remove.Shows[0].Seasons[0].Episodes) != 2 {
		t.Fatalf("remove payload = %+v", remove.Shows)
	}
}

func TestHistoryEpisodeWithRealIDsKeepsEpisodeIDs(t *testing.T) {
	t.Parallel()
	play := episodePlay(traktIDs{TVDB: 54321}, traktIDs{TMDB: 999}, 2, 5, time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC))
	add, _ := json.Marshal(historyPayload([]localPlay{play}))
	if want := `{"episodes":[{"watched_at":"2026-05-04T12:00:00Z","ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":54321}}]}`; string(add) != want {
		t.Fatalf("add payload = %s\nwant %s", add, want)
	}
	remove, _ := json.Marshal(historyRemovePayload([]localPlay{play}))
	if !bytes.Contains(remove, []byte(`"episodes":[{"ids":{"trakt":0,"slug":"","imdb":"","tmdb":0,"tvdb":54321}}]`)) || bytes.Contains(remove, []byte("shows")) {
		t.Fatalf("remove payload = %s", remove)
	}
}
