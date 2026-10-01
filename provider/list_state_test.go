package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

var (
	kindWatched   = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED
	kindProgress  = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS
	kindFavorite  = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE
	kindWatchlist = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST
	kindRating    = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING
	kindDropped   = pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED
)

// hostRow is what the host reads from a remote state (remoteIdentityFromProto
// plus the typed state), in the built-in provider's field names, so golden
// rows from the built-in tests compare directly.
type hostRow struct {
	Key, Kind, Title                               string
	Year                                           int
	IMDbID, TMDBID, TVDBID                         string
	SeriesTitle                                    string
	SeriesYear                                     int
	SeriesIMDbID, SeriesTMDBID, SeriesTVDBID       string
	SeasonNumber, EpisodeNumber, PlayCount, Rating int
	LastWatchedAt, PausedAt, FavoritedAt, RatedAt  time.Time
	DroppedAt                                      time.Time
	ProgressPercent                                float64
	Favorite, Watchlist, Dropped                   bool
}

func hostRowFrom(state *pluginv1.WatchSyncRemoteState) hostRow {
	media := state.GetMedia()
	row := hostRow{
		Key: state.GetProviderItemKey(), Kind: mediaKind(media), Title: media.GetTitle(), Year: int(media.GetYear()),
		IMDbID: media.GetExternalIds()["imdb"], TMDBID: media.GetExternalIds()["tmdb"], TVDBID: media.GetExternalIds()["tvdb"],
		SeriesTitle: media.GetSeriesTitle(), SeriesYear: int(media.GetSeriesYear()),
		SeriesIMDbID: media.GetSeriesExternalIds()["imdb"], SeriesTMDBID: media.GetSeriesExternalIds()["tmdb"], SeriesTVDBID: media.GetSeriesExternalIds()["tvdb"],
		SeasonNumber: int(media.GetSeasonNumber()), EpisodeNumber: int(media.GetEpisodeNumber()),
	}
	if watched := state.GetWatched(); watched != nil {
		row.PlayCount, row.LastWatchedAt = int(watched.GetPlayCount()), watched.GetLastWatchedAt().AsTime()
	}
	if progress := state.GetProgress(); progress != nil {
		row.ProgressPercent, row.PausedAt = progress.GetProgressPercent(), progress.GetPausedAt().AsTime()
	}
	if favorite := state.GetFavorite(); favorite != nil {
		row.Favorite, row.FavoritedAt = true, favorite.GetListedAt().AsTime()
	}
	if watchlist := state.GetWatchlist(); watchlist != nil {
		row.Watchlist, row.FavoritedAt = true, watchlist.GetListedAt().AsTime()
	}
	if rating := state.GetRating(); rating != nil {
		row.Rating, row.RatedAt = int(rating.GetRating()), rating.GetRatedAt().AsTime()
	}
	if dropped := state.GetDropped(); dropped != nil {
		row.Dropped = true
		// The host leaves the drop time unknown without a valid listed_at.
		if listedAt := dropped.GetListedAt(); listedAt.CheckValid() == nil {
			row.DroppedAt = listedAt.AsTime()
		}
	}
	return row
}

func hostRows(items []*pluginv1.WatchSyncRemoteState) []hostRow {
	rows := make([]hostRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, hostRowFrom(item))
	}
	return rows
}

func day(month time.Month, d int) time.Time { return time.Date(2026, month, d, 12, 0, 0, 0, time.UTC) }

func TestListWatchedImportsEveryPageAndEpisodeProgress(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var requests []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := strings.TrimPrefix(r.URL.Path, "/sync/watched/")
		page := r.URL.Query().Get("page")
		mu.Lock()
		requests = append(requests, kind+":"+page)
		mu.Unlock()
		if r.URL.Query().Get("limit") != "250" || r.Header.Get("Authorization") != "Bearer "+testAccessToken {
			t.Errorf("query = %s, authorization = %q", r.URL.RawQuery, r.Header.Get("Authorization"))
		}
		// Short pages without pagination headers: only the empty page ends.
		switch page {
		case "1", "2":
			switch kind {
			case "movies":
				writeFixture(t, w, `[{"plays":2,"last_watched_at":"2026-09-01T12:00:00Z","movie":{"title":"Movie %s","year":2020,"ids":{"tmdb":10%s}}}]`, page, page)
			case "shows":
				if r.URL.Query().Get("extended") != "progress" {
					// Current Trakt default: no seasons or episodes without progress.
					writeFixture(t, w, `[{"show":{"title":"Show","ids":{"tmdb":200}}}]`)
					return
				}
				writeFixture(t, w, `[{"show":{"title":"Show %s","year":2021,"ids":{"tmdb":20%s,"tvdb":30%s,"imdb":"tt40%s"}},"seasons":[{"number":0,"episodes":[{"number":1,"plays":3,"last_watched_at":"2026-09-02T12:00:00Z"}]},{"number":2,"episodes":[{"number":5,"plays":1,"last_watched_at":"2026-09-03T12:00:00Z"}]}]}]`, page, page, page, page)
			default:
				t.Errorf("unexpected path %s", r.URL.Path)
				http.NotFound(w, r)
			}
		case "3":
			writeFixture(t, w, `[]`)
		default:
			t.Errorf("unexpected page %q", page)
			http.Error(w, "unexpected page", http.StatusInternalServerError)
		}
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	result := traverse(t, server, kindWatched)
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	wantRequests := []string{"movies:1", "movies:2", "movies:3", "movies:1", "movies:2", "movies:3", "shows:1", "shows:2", "shows:3", "shows:1", "shows:2", "shows:3"}
	if !reflect.DeepEqual(requests, wantRequests) {
		t.Fatalf("requests = %v, want %v", requests, wantRequests)
	}
	episode := func(show string, season, number, plays, watched int) hostRow {
		return hostRow{
			Key: "show:tvdb:30" + show + ":s" + strconv.Itoa(season) + ":e" + strconv.Itoa(number), Kind: kindEpisode,
			SeriesTitle: "Show " + show, SeriesYear: 2021, SeriesIMDbID: "tt40" + show, SeriesTMDBID: "20" + show, SeriesTVDBID: "30" + show,
			SeasonNumber: season, EpisodeNumber: number, PlayCount: plays, LastWatchedAt: day(time.September, watched),
		}
	}
	want := []hostRow{
		{Key: "tmdb:101", Kind: kindMovie, Title: "Movie 1", Year: 2020, TMDBID: "101", PlayCount: 2, LastWatchedAt: day(time.September, 1)},
		{Key: "tmdb:102", Kind: kindMovie, Title: "Movie 2", Year: 2020, TMDBID: "102", PlayCount: 2, LastWatchedAt: day(time.September, 1)},
		episode("1", 0, 1, 3, 2), episode("1", 2, 5, 1, 3),
		episode("2", 0, 1, 3, 2), episode("2", 2, 5, 1, 3),
	}
	if got := hostRows(result.items); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows =\n%+v\nwant\n%+v", got, want)
	}
}

func TestListWatchedDoesNotReturnPartialHistoryOnLaterPageFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"movies", "shows"} {
		for _, failure := range []string{"http", "json"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				t.Parallel()
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("page") == "1" {
						writeFixture(t, w, `[{"plays":1,"last_watched_at":"2026-09-01T12:00:00Z","movie":{"ids":{"tmdb":123}},"show":{"ids":{"tmdb":456}},"seasons":[{"number":1,"episodes":[{"number":1,"plays":1,"last_watched_at":"2026-09-01T12:00:00Z"}]}]}]`)
						return
					}
					if r.URL.Path == "/sync/watched/"+kind {
						if failure == "http" {
							http.Error(w, "unavailable", http.StatusServiceUnavailable)
						} else {
							writeFixture(t, w, `[{`)
						}
						return
					}
					writeFixture(t, w, `[]`)
				}))
				defer upstream.Close()
				server, _ := newTestServer(t, upstream)

				result := traverse(t, server, kindWatched)
				if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.items != nil {
					t.Fatalf("result = %+v, want a TEMPORARY fault and no rows", result)
				}
			})
		}
	}
}

type listCase struct {
	name       string
	kind       pluginv1.WatchSyncRemoteStateKind
	moviesPath string
	showsPath  string
}

func listCases() []listCase {
	return []listCase{
		{"favorites", kindFavorite, "/users/me/favorites/movies/added", "/users/me/favorites/shows/added"},
		{"watchlist", kindWatchlist, "/sync/watchlist/movies", "/sync/watchlist/shows"},
	}
}

func TestListFavoritesAndWatchlistImportEveryPageAsCompleteSnapshots(t *testing.T) {
	t.Parallel()
	for _, tc := range listCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var requests []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				kind := map[string]string{tc.moviesPath: "movies", tc.showsPath: "shows"}[r.URL.Path]
				if kind == "" {
					t.Errorf("unexpected path %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				page := r.URL.Query().Get("page")
				mu.Lock()
				requests = append(requests, kind+":"+page)
				mu.Unlock()
				switch page {
				case "1", "2":
					if kind == "movies" {
						writeFixture(t, w, `[{"listed_at":"2026-05-0%sT12:00:00Z","movie":{"title":"Movie %s","year":2020,"ids":{"tmdb":10%s}}}]`, page, page, page)
					} else {
						writeFixture(t, w, `[{"listed_at":"2026-06-0%sT12:00:00Z","show":{"title":"Show %s","year":2021,"ids":{"tvdb":30%s,"tmdb":20%s}}}]`, page, page, page, page)
					}
				case "3":
					writeFixture(t, w, `[]`)
				default:
					t.Errorf("unexpected page %q", page)
					http.Error(w, "unexpected page", http.StatusInternalServerError)
				}
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			result := traverse(t, server, tc.kind)
			if result.fault != nil {
				t.Fatal(faultText(result.fault))
			}
			wantRequests := []string{"movies:1", "movies:2", "movies:3", "movies:1", "movies:2", "movies:3", "shows:1", "shows:2", "shows:3", "shows:1", "shows:2", "shows:3"}
			if !reflect.DeepEqual(requests, wantRequests) {
				t.Fatalf("requests = %v, want %v", requests, wantRequests)
			}
			favorite, watchlist := tc.kind == kindFavorite, tc.kind == kindWatchlist
			want := []hostRow{
				{Key: "tmdb:101", Kind: kindMovie, Title: "Movie 1", Year: 2020, TMDBID: "101", FavoritedAt: day(time.May, 1), Favorite: favorite, Watchlist: watchlist},
				{Key: "tmdb:102", Kind: kindMovie, Title: "Movie 2", Year: 2020, TMDBID: "102", FavoritedAt: day(time.May, 2), Favorite: favorite, Watchlist: watchlist},
				{Key: "tvdb:301", Kind: kindSeries, Title: "Show 1", Year: 2021, TMDBID: "201", TVDBID: "301", FavoritedAt: day(time.June, 1), Favorite: favorite, Watchlist: watchlist},
				{Key: "tvdb:302", Kind: kindSeries, Title: "Show 2", Year: 2021, TMDBID: "202", TVDBID: "302", FavoritedAt: day(time.June, 2), Favorite: favorite, Watchlist: watchlist},
			}
			if got := hostRows(result.items); !reflect.DeepEqual(got, want) {
				t.Fatalf("rows =\n%+v\nwant\n%+v", got, want)
			}
		})
	}
}

func TestListsDoNotReturnPartialResultsOnLaterPageFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range listCases() {
		for _, kind := range []string{"movies", "shows"} {
			for _, failure := range []string{"http", "json"} {
				t.Run(tc.name+"/"+kind+"/"+failure, func(t *testing.T) {
					t.Parallel()
					failingPath := tc.moviesPath
					if kind == "shows" {
						failingPath = tc.showsPath
					}
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Query().Get("page") == "1" {
							writeFixture(t, w, `[{"listed_at":"2026-05-01T12:00:00Z","movie":{"ids":{"tmdb":123}},"show":{"ids":{"tvdb":456}}}]`)
							return
						}
						if r.URL.Path == failingPath {
							if failure == "http" {
								http.Error(w, "unavailable", http.StatusServiceUnavailable)
							} else {
								writeFixture(t, w, `[{`)
							}
							return
						}
						writeFixture(t, w, `[]`)
					}))
					defer upstream.Close()
					server, _ := newTestServer(t, upstream)

					result := traverse(t, server, tc.kind)
					if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.items != nil {
						t.Fatalf("result = %+v, want a TEMPORARY fault and no rows", result)
					}
				})
			}
		}
	}
}

// runListing reads one custom listing to completion inside one call, as a
// traversal does.
func runListing(t *testing.T, server *Server, l listing) traversalResult {
	t.Helper()
	ctx, cancel, stopAt := server.window(context.Background())
	defer cancel()
	client := &apiClient{server: server, app: appConfig{clientID: testClientID, clientSecret: testClientSecret}, token: testAccessToken}
	tr := &traversal{client: client, plan: []listing{l}, token: traversalToken{listingRead: freshRead(0)}, stopAt: stopAt}
	response := tr.run(ctx)
	if response.GetNextPageToken() != "" {
		t.Fatalf("listing paused: %s", response.GetNextPageToken())
	}
	return traversalResult{items: response.GetItems(), fault: response.GetFault(), calls: 1}
}

func watchlistMovies(query url.Values) listing {
	return listing{name: "watchlist movies", path: "/sync/watchlist/movies", query: query, states: listedMovieStates(watchlistList)}
}

func TestListingStopsOnPaginationHeaders(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		headers   map[string]string
		bodies    map[string]string
		wantPages []string
		wantRows  int
	}{
		{
			// Both pages are shorter than the limit; the page count decides.
			name:      "page count",
			headers:   map[string]string{"X-Pagination-Limit": "250", "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"},
			bodies:    map[string]string{"1": `[{"movie":{"ids":{"tmdb":1}}}]`, "2": `[{"movie":{"ids":{"tmdb":2}}}]`},
			wantPages: []string{"1", "2", "1", "2"},
			wantRows:  2,
		},
		{
			// Trakt applied a smaller limit than requested; a shorter page is the last.
			name:      "applied limit",
			headers:   map[string]string{"X-Pagination-Limit": "2"},
			bodies:    map[string]string{"1": `[{"movie":{"ids":{"tmdb":1}}},{"movie":{"ids":{"tmdb":2}}}]`, "2": `[{"movie":{"ids":{"tmdb":3}}}]`},
			wantPages: []string{"1", "2", "1", "2"},
			wantRows:  3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var pages []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				pages = append(pages, page)
				if got := r.URL.Query().Get("extended"); got != "full" {
					t.Errorf("extended = %q, want full", got)
				}
				if got := r.URL.Query().Get("limit"); got != "250" {
					t.Errorf("limit = %q, want 250", got)
				}
				body, ok := tc.bodies[page]
				if !ok {
					t.Errorf("unexpected page %q", page)
					http.Error(w, "unexpected page", http.StatusInternalServerError)
					return
				}
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				writeFixture(t, w, "%s", body)
			}))
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)

			query := url.Values{"extended": {"full"}, "page": {"9"}, "limit": {"5"}}
			result := runListing(t, server, watchlistMovies(query))
			if result.fault != nil {
				t.Fatal(faultText(result.fault))
			}
			if !reflect.DeepEqual(pages, tc.wantPages) || len(result.items) != tc.wantRows {
				t.Fatalf("pages = %v, rows = %d; want %v, %d", pages, len(result.items), tc.wantPages, tc.wantRows)
			}
			if query.Get("page") != "9" || query.Get("limit") != "5" {
				t.Fatalf("listing query was modified: %v", query)
			}
		})
	}
}

func TestListingFailsAtThePageCap(t *testing.T) {
	t.Parallel()
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		// A server that ignores page and sends no pagination headers never ends.
		writeFixture(t, w, `[{"movie":{"ids":{"tmdb":1}}}]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	server.pages = newCredentialLimiter(time.Nanosecond, traktMaxPages)
	server.responseBytes = 1 << 30

	result := runListing(t, server, watchlistMovies(nil))
	if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.items != nil {
		t.Fatalf("result = %+v, want a TEMPORARY fault and no rows", result)
	}
	if requests != traktMaxPages {
		t.Fatalf("requests = %d, want %d", requests, traktMaxPages)
	}
}

func TestListingFailsWhenTheListChangesMidRead(t *testing.T) {
	t.Parallel()
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		// The list shrinks between page 1 and page 2, which shifts offsets.
		if r.URL.Query().Get("page") == "1" {
			w.Header().Set("X-Pagination-Item-Count", "251")
			w.Header().Set("X-Pagination-Page-Count", "2")
			writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"A","ids":{"trakt":1,"tmdb":1}}}]`)
			return
		}
		w.Header().Set("X-Pagination-Item-Count", "250")
		w.Header().Set("X-Pagination-Page-Count", "1")
		writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"B","ids":{"trakt":2,"tmdb":2}}}]`)
	}))
	defer upstream.Close()
	server, sleeps := newTestServer(t, upstream)

	result := traverse(t, server, kindWatchlist)
	if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.items != nil {
		t.Fatalf("result = %+v, want a TEMPORARY fault and no rows", result)
	}
	// Each of the three attempts fails on page 2.
	if requests != 6 || !reflect.DeepEqual(sleeps.recorded(), []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("requests = %d, waits = %v; want 6 requests and waits of 1s then 2s", requests, sleeps.recorded())
	}
}

func TestListingFailsWhenAnEqualCountChangeShiftsPages(t *testing.T) {
	t.Parallel()
	// Each pass replaces a title while keeping the count equal, so every
	// verification read must reject the shifted page even after restarting.
	pass, requests := 0, 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		page := r.URL.Query().Get("page")
		if page == "1" {
			pass++
		}
		w.Header().Set("X-Pagination-Item-Count", "251")
		w.Header().Set("X-Pagination-Page-Count", "2")
		switch {
		case page == "1":
			writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"A","ids":{"trakt":1,"tmdb":1}}}]`)
		case pass%2 == 1:
			writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"B","ids":{"trakt":2,"tmdb":2}}}]`)
		default:
			writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"C","ids":{"trakt":3,"tmdb":3}}}]`)
		}
	}))
	defer upstream.Close()
	server, sleeps := newTestServer(t, upstream)

	result := runListing(t, server, watchlistMovies(nil))
	if result.fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || result.items != nil {
		t.Fatalf("result = %+v, want a TEMPORARY fault and no rows", result)
	}
	// Each of the three attempts reads two passes of two pages.
	if requests != 12 || !reflect.DeepEqual(sleeps.recorded(), []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("requests = %d, waits = %v; want 12 requests and waits of 1s then 2s", requests, sleeps.recorded())
	}
}

// A restart in place discards the rows of the inconsistent attempt.
func TestListingRestartsInPlaceAndReturnsOnlyTheSettledRead(t *testing.T) {
	t.Parallel()
	pass := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "1" {
			pass++
		}
		w.Header().Set("X-Pagination-Page-Count", "2")
		title := "A"
		if page == "1" && pass == 1 {
			title = "Z" // replaced before the verification pass
		}
		if page == "2" {
			title = "B"
		}
		writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"%s","ids":{"tmdb":1}}}]`, title)
	}))
	defer upstream.Close()
	server, sleeps := newTestServer(t, upstream)

	result := runListing(t, server, watchlistMovies(nil))
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	var titles []string
	for _, item := range result.items {
		titles = append(titles, item.GetMedia().GetTitle())
	}
	if !reflect.DeepEqual(titles, []string{"A", "B"}) || !reflect.DeepEqual(sleeps.recorded(), []time.Duration{time.Second}) {
		t.Fatalf("titles = %v, waits = %v", titles, sleeps.recorded())
	}
}

func TestListingReadsASinglePageOnce(t *testing.T) {
	t.Parallel()
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("X-Pagination-Page-Count", "1")
		writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"A","ids":{"trakt":1,"tmdb":1}}}]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	result := runListing(t, server, watchlistMovies(nil))
	if result.fault != nil || len(result.items) != 1 || requests != 1 {
		t.Fatalf("rows = %d, requests = %d, fault = %v; want one row from one request", len(result.items), requests, result.fault)
	}
}

// The page limiter stops a read before the call's budget runs out: the call
// returns what it read, and a call that cannot read at all defers the
// connection like a 429.
func TestListingPacesPagedReadsPerToken(t *testing.T) {
	t.Parallel()
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("X-Pagination-Page-Count", "3")
		writeFixture(t, w, `[{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"A","ids":{"trakt":1,"tmdb":1}}}]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	// Two pages at once, then one per hour.
	server.pages = newCredentialLimiter(time.Hour, 2)

	first, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(testAccessToken), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist},
	})
	if err != nil || first.GetFault() != nil || len(first.GetItems()) != 2 || first.GetNextPageToken() == "" {
		t.Fatalf("first call = %v, %v; want two rows and a page token", first, err)
	}
	second, _ := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(testAccessToken), PageToken: first.GetNextPageToken(), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist},
	})
	if wait := requireRateLimited(t, second.GetFault()); wait != pageInterval {
		t.Fatalf("retry after = %s, want %s", wait, pageInterval)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
	// Another token has its own budget.
	other, _ := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext("other-token"), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist},
	})
	if other.GetFault() != nil || len(other.GetItems()) != 2 {
		t.Fatalf("other token = %v", other)
	}
}

func TestTraktPageBudgetStaysUnderTheGETLimit(t *testing.T) {
	t.Parallel()
	// Trakt allows 500 authenticated GETs per five minutes.
	if perWindow := pageBurst + int((5*time.Minute)/pageInterval); perWindow >= 500 {
		t.Fatalf("paged reads allow %d GETs in five minutes, want fewer than 500", perWindow)
	}
}

// fakePagedList serves a watchlist of movies a few rows per page, with
// pagination headers, and an empty show watchlist. Tests change rows between
// calls.
type fakePagedList struct {
	mu       sync.Mutex
	rows     []string
	perPage  int
	requests []string
}

func (f *fakePagedList) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path != "/sync/watchlist/movies" {
		_, _ = w.Write([]byte(`[]`))
		return
	}
	page := 1
	if value := r.URL.Query().Get("page"); value != "" {
		page = parseInt(value)
	}
	f.requests = append(f.requests, r.URL.Query().Get("page"))
	pages := (len(f.rows) + f.perPage - 1) / f.perPage
	w.Header().Set("X-Pagination-Page-Count", intString(pages))
	w.Header().Set("X-Pagination-Item-Count", intString(len(f.rows)))
	var out []json.RawMessage
	for index := (page - 1) * f.perPage; index < min(len(f.rows), page*f.perPage); index++ {
		out = append(out, json.RawMessage(`{"listed_at":"2026-01-01T00:00:00Z","movie":{"title":"`+f.rows[index]+`","ids":{"tmdb":`+intString(index+1)+`}}}`))
	}
	if out == nil {
		out = []json.RawMessage{}
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (f *fakePagedList) set(rows ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = rows
}

func titlesOf(items []*pluginv1.WatchSyncRemoteState) []string {
	titles := make([]string, 0, len(items))
	for _, item := range items {
		titles = append(titles, item.GetMedia().GetTitle())
	}
	return titles
}

// A listing too large for one response continues on later calls through the
// page token, and its verification pass still covers every row.
func TestListingContinuesAcrossCallsAndVerifiesEveryRow(t *testing.T) {
	t.Parallel()
	fake := &fakePagedList{rows: []string{"A", "B", "C", "D", "E"}, perPage: 2}
	upstream := httptest.NewServer(fake)
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	// Every response holds one row, so pages split across calls.
	server.responseBytes = 1

	result := traverse(t, server, kindWatchlist)
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	if titles := titlesOf(result.items); !reflect.DeepEqual(titles, []string{"A", "B", "C", "D", "E"}) {
		t.Fatalf("titles = %v", titles)
	}
	if result.calls < 5 {
		t.Fatalf("calls = %d, want the read spread over several calls", result.calls)
	}
	// Pass one re-reads a split page once per extra call; pass two reads each
	// page once.
	wantRequests := []string{"1", "1", "2", "2", "3", "1", "2", "3"}
	if !reflect.DeepEqual(fake.requests, wantRequests) {
		t.Fatalf("requests = %v, want %v", fake.requests, wantRequests)
	}
}

// Rows already returned to the host cannot be withdrawn, so a change found
// on a later call fails the traversal for the next sync to repeat.
func TestListingThatChangesBetweenCallsFailsTheTraversal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		perPage int
		change  func(*fakePagedList)
		after   int
	}{
		// A one-page listing has no verification pass; the split page's
		// digest is its only check.
		{"split single page changed", 10, func(f *fakePagedList) { f.set("Z", "B", "C", "D", "E") }, 1},
		{"split page changed", 2, func(f *fakePagedList) { f.set("Z", "B", "C", "D", "E") }, 1},
		{"count changed", 2, func(f *fakePagedList) { f.set("A", "B", "C", "D", "E", "F") }, 2},
		{"equal count replacement before verification", 2, func(f *fakePagedList) { f.set("A", "B", "C", "D", "Y") }, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakePagedList{rows: []string{"A", "B", "C", "D", "E"}, perPage: tc.perPage}
			upstream := httptest.NewServer(fake)
			defer upstream.Close()
			server, _ := newTestServer(t, upstream)
			server.responseBytes = 1

			pageToken := ""
			for call := 1; ; call++ {
				if call == tc.after+1 {
					tc.change(fake)
				}
				response, _ := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
					Context: authContext(testAccessToken), PageToken: pageToken, StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist},
				})
				if response.GetFault() != nil {
					if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY || call <= tc.after {
						t.Fatalf("call %d: fault = %v", call, response.GetFault())
					}
					return
				}
				if pageToken = response.GetNextPageToken(); pageToken == "" {
					t.Fatal("traversal completed over a changed list")
				}
			}
		})
	}
}

func TestPageTokensAreBoundToTheirTraversal(t *testing.T) {
	t.Parallel()
	fake := &fakePagedList{rows: []string{"A", "B", "C"}, perPage: 1}
	upstream := httptest.NewServer(fake)
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)
	server.responseBytes = 1

	first, _ := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(testAccessToken), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist},
	})
	if first.GetNextPageToken() == "" {
		t.Fatalf("first call = %v", first)
	}
	for name, request := range map[string]*pluginv1.WatchSyncListRemoteStateRequest{
		"other family": {PageToken: first.GetNextPageToken(), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindFavorite}},
		"garbage":      {PageToken: "not a token", StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist}},
		"forged":       {PageToken: encodeTraversal(traversalToken{Plan: "4", listingRead: listingRead{Pass: 2, Page: 1}}), StateKinds: []pluginv1.WatchSyncRemoteStateKind{kindWatchlist}},
	} {
		request.Context = authContext(testAccessToken)
		response, _ := server.ListRemoteState(context.Background(), request)
		if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
			t.Errorf("%s: fault = %v", name, response.GetFault())
		}
	}
	response, _ := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(testAccessToken), StateKinds: []pluginv1.WatchSyncRemoteStateKind{pluginv1.WatchSyncRemoteStateKind(99)},
	})
	if response.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST {
		t.Fatalf("unknown family: fault = %v", response.GetFault())
	}
}

func TestListProgressReadsPlaybackInOneRequest(t *testing.T) {
	t.Parallel()
	var queries []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/playback" {
			t.Errorf("path = %s", r.URL.Path)
		}
		queries = append(queries, r.URL.RawQuery)
		writeFixture(t, w, `[
			{"progress":41.5,"paused_at":"2026-05-04T12:00:00.000Z","type":"movie","movie":{"title":"Heat","year":1995,"ids":{"trakt":1,"imdb":"tt0113277","tmdb":949}}},
			{"progress":100,"paused_at":"2026-05-05T12:00:00.000Z","type":"episode","episode":{"season":2,"number":5,"title":"Ep","year":2003,"ids":{"trakt":7,"tvdb":501,"tmdb":601,"imdb":"tt9"}},"show":{"title":"The Wire","year":2002,"ids":{"trakt":2,"imdb":"tt0306414","tmdb":1438,"tvdb":79126}}},
			{"progress":10,"paused_at":"2026-05-06T12:00:00.000Z","type":"season"}
		]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	result := traverse(t, server, kindProgress)
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	if !reflect.DeepEqual(queries, []string{""}) {
		t.Fatalf("queries = %q, want one request without page or limit", queries)
	}
	want := []hostRow{
		{Key: "imdb:tt0113277", Kind: kindMovie, Title: "Heat", Year: 1995, IMDbID: "tt0113277", TMDBID: "949", ProgressPercent: 41.5, PausedAt: day(time.May, 4)},
		{
			Key: "tvdb:501", Kind: kindEpisode, Title: "Ep", Year: 2003, IMDbID: "tt9", TMDBID: "601", TVDBID: "501",
			SeriesTitle: "The Wire", SeriesYear: 2002, SeriesIMDbID: "tt0306414", SeriesTMDBID: "1438", SeriesTVDBID: "79126",
			SeasonNumber: 2, EpisodeNumber: 5, ProgressPercent: maxProgressPercent, PausedAt: day(time.May, 5),
		},
	}
	if got := hostRows(result.items); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows =\n%+v\nwant\n%+v", got, want)
	}
}

func TestListRatingsReadsMoviesAndShowsAsCompleteSnapshots(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "250" {
			t.Errorf("%s limit = %q, want 250", r.URL.Path, r.URL.Query().Get("limit"))
		}
		w.Header().Set("X-Pagination-Page-Count", "1")
		switch r.URL.Path {
		case "/sync/ratings/movies":
			writeFixture(t, w, `[{"rated_at":"2026-03-01T10:00:00.000Z","rating":7,"type":"movie","movie":{"title":"Heat","year":1995,"ids":{"trakt":1,"imdb":"tt0113277","tmdb":949}}}]`)
		case "/sync/ratings/shows":
			writeFixture(t, w, `[{"rated_at":"2026-03-02T10:00:00.000Z","rating":10,"type":"show","show":{"title":"The Wire","year":2002,"ids":{"trakt":2,"imdb":"tt0306414","tmdb":1438,"tvdb":79126}}}]`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	result := traverse(t, server, kindRating)
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	want := []hostRow{
		{Key: "imdb:tt0113277", Kind: kindMovie, Title: "Heat", Year: 1995, IMDbID: "tt0113277", TMDBID: "949", Rating: 7, RatedAt: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)},
		{Key: "tvdb:79126", Kind: kindSeries, Title: "The Wire", Year: 2002, IMDbID: "tt0306414", TMDBID: "1438", TVDBID: "79126", Rating: 10, RatedAt: time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC)},
	}
	if got := hostRows(result.items); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows =\n%+v\nwant\n%+v", got, want)
	}
}

// Ported from the built-in provider's TestFetchDroppedReadsDroppedShowsAsCompleteSnapshot.
// Rows that are not shows are skipped, and a row without hidden_at leaves the
// drop time unknown.
func TestListDroppedReadsDroppedShowsAsCompleteSnapshot(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/users/hidden/dropped" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("limit") != "250" || r.URL.Query().Get("page") != "1" || r.URL.Query().Has("type") {
			t.Errorf("query = %s, want limit 250, page 1, and no type", r.URL.RawQuery)
		}
		w.Header().Set("X-Pagination-Page-Count", "1")
		writeFixture(t, w, `[
			{"hidden_at":"2026-03-01T10:00:00.000Z","type":"show","show":{"title":"Rick and Morty","year":2013,"ids":{"trakt":69293,"imdb":"tt2861424","tmdb":60625,"tvdb":275274}}},
			{"hidden_at":"2026-03-02T10:00:00.000Z","type":"movie","movie":{"title":"Heat","ids":{"trakt":1}}},
			{"show":{"title":"Severance","year":2022,"ids":{"trakt":154997,"imdb":"tt11280740","tmdb":95396}}}
		]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	result := traverse(t, server, kindDropped)
	if result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	if result.calls != 1 {
		t.Fatalf("calls = %d, want one", result.calls)
	}
	want := []hostRow{
		{
			Key: "tvdb:275274", Kind: kindSeries, Title: "Rick and Morty", Year: 2013, IMDbID: "tt2861424", TMDBID: "60625", TVDBID: "275274",
			Dropped: true, DroppedAt: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC),
		},
		{Key: "tmdb:95396", Kind: kindSeries, Title: "Severance", Year: 2022, IMDbID: "tt11280740", TMDBID: "95396", Dropped: true},
	}
	if got := hostRows(result.items); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows =\n%+v\nwant\n%+v", got, want)
	}
	if listedAt := result.items[1].GetDropped().GetListedAt(); listedAt != nil {
		t.Fatalf("listed_at = %v, want none for a row without hidden_at", listedAt)
	}
}

// With no state family named, a traversal reads every advertised family in
// one snapshot.
func TestListEveryFamilyWhenNoneIsNamed(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	paths := map[string]bool{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.URL.Path] = true
		mu.Unlock()
		writeFixture(t, w, `[]`)
	}))
	defer upstream.Close()
	server, _ := newTestServer(t, upstream)

	if result := traverse(t, server); result.fault != nil {
		t.Fatal(faultText(result.fault))
	}
	for _, kind := range allStateKinds {
		for _, l := range listingsFor(kind) {
			if !paths[l.path] {
				t.Errorf("%s was not read", l.path)
			}
		}
	}
}
