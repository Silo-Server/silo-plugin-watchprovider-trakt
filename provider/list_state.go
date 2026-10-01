package provider

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Reading Trakt listings
//
// Trakt pages listings by offset, so a page shifts when the list changes
// mid-read, which can skip or repeat a row; the host reads a skipped favorite,
// watchlist entry, or rating as removed. Like Silo's built-in provider, a
// listing that spans several pages is therefore read twice, and the read fails
// unless both passes return the same rows; a changed X-Pagination-Item-Count
// within a pass fails it too.
//
// Both passes run inside one ListRemoteState call while they fit its time and
// size budget, and an inconsistent read then restarts in place a bounded
// number of times. A listing too large for one call continues on later calls:
// the stateless page token carries the read position and running digests of
// both passes, the second pass only verifies, and a listing found to have
// changed fails the traversal with TEMPORARY so the next sync reads it again.
// Each upstream page is read once per pass, so the plugin spends the same
// GETs as the built-in provider. A response therefore holds whole upstream
// pages, bounded by Server.responseBytes, rather than page_size items: cutting
// a 250-row page into page_size slices would re-read it for every slice.

const (
	// traktPageLimit is Trakt's maximum page size. Larger limits are clamped.
	traktPageLimit = 250
	// traktMaxPages bounds a listing whose last page is never detected, such
	// as a server that ignores page and sends no pagination headers.
	traktMaxPages = 1000
	// An inconsistent listing is read at most this many times in one call.
	traktReadAttempts = 3

	// maxProgressPercent keeps resume progress inside the host's [0, 100).
	maxProgressPercent = 99.999
)

// listing is one Trakt listing a traversal reads.
type listing struct {
	name  string
	path  string
	query url.Values
	// unpaged listings are read in one request without page or limit.
	unpaged bool
	// states converts one listed row.
	states func(json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error)
}

// listingsFor returns the listings that hold one state family, in the order
// Silo's built-in provider read them.
func listingsFor(kind pluginv1.WatchSyncRemoteStateKind) []listing {
	switch kind {
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:
		return []listing{
			{name: "watched movies", path: "/sync/watched/movies", states: watchedMovieStates},
			// Season and episode watched data is not included without
			// extended=progress.
			{name: "watched shows", path: "/sync/watched/shows", query: url.Values{"extended": {"progress"}}, states: watchedShowStates},
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS:
		return []listing{{name: "playback progress", path: "/sync/playback", unpaged: true, states: playbackStates}}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE:
		return []listing{
			{name: "favorite movies", path: "/users/me/favorites/movies/added", states: listedMovieStates(favoriteList)},
			{name: "favorite shows", path: "/users/me/favorites/shows/added", states: listedShowStates(favoriteList)},
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST:
		return []listing{
			{name: "watchlist movies", path: "/sync/watchlist/movies", states: listedMovieStates(watchlistList)},
			{name: "watchlist shows", path: "/sync/watchlist/shows", states: listedShowStates(watchlistList)},
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING:
		return []listing{
			{name: "movie ratings", path: "/sync/ratings/movies", states: ratedMovieStates},
			{name: "show ratings", path: "/sync/ratings/shows", states: ratedShowStates},
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED:
		return []listing{{name: "dropped shows", path: "/users/hidden/dropped", states: droppedShowStates}}
	default:
		return nil
	}
}

// allStateKinds are the families an empty state_kinds request asks for.
var allStateKinds = []pluginv1.WatchSyncRemoteStateKind{
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING,
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED,
}

// traversalPlan returns the listings for the requested families and a
// signature that ties page tokens to them.
func traversalPlan(kinds []pluginv1.WatchSyncRemoteStateKind) ([]listing, string, *pluginv1.WatchSyncFault) {
	if len(kinds) == 0 {
		kinds = allStateKinds
	}
	var plan []listing
	signature := make([]string, 0, len(kinds))
	seen := make(map[pluginv1.WatchSyncRemoteStateKind]bool, len(kinds))
	for _, kind := range kinds {
		if seen[kind] {
			continue
		}
		seen[kind] = true
		listings := listingsFor(kind)
		if listings == nil {
			return nil, "", invalidRequestFault("Trakt does not support the requested state family")
		}
		plan = append(plan, listings...)
		signature = append(signature, strconv.Itoa(int(kind)))
	}
	return plan, strings.Join(signature, ","), nil
}

// traversalToken is the page token: the listing being read and the position
// of its read.
type traversalToken struct {
	Plan    string `json:"plan"`
	Listing int    `json:"listing"`
	listingRead
}

// listingRead is the stateless position of one listing's two-pass read.
type listingRead struct {
	// Pass 1 returns rows; pass 2 reads them again to verify them.
	Pass int `json:"pass"`
	// Page is the upstream page to read next, from 1.
	Page int `json:"page"`
	// Skip counts the rows of Page already returned, when a page did not fit
	// in one response, and PageSum is the digest of that page's rows.
	Skip    int    `json:"skip,omitempty"`
	PageSum string `json:"page_sum,omitempty"`
	// Count is the X-Pagination-Item-Count this pass has seen.
	Count int `json:"count,omitempty"`
	// First and Second are running digests of every row of each pass.
	First  string `json:"first,omitempty"`
	Second string `json:"second,omitempty"`
	// Attempt counts in-place restarts of an inconsistent read.
	Attempt int `json:"attempt,omitempty"`
}

func freshRead(attempt int) listingRead {
	return listingRead{Pass: 1, Page: 1, Attempt: attempt}
}

// unreturned reports whether none of the listing's rows has reached the host,
// so an inconsistent read may still restart.
func (r listingRead) unreturned() bool {
	return r.Pass == 1 && r.Page == 1 && r.Skip == 0
}

func decodeTraversal(value, signature string, listings int) (traversalToken, *pluginv1.WatchSyncFault) {
	if strings.TrimSpace(value) == "" {
		return traversalToken{Plan: signature, listingRead: freshRead(0)}, nil
	}
	invalid := invalidRequestFault("Trakt page token is invalid")
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return traversalToken{}, invalid
	}
	var token traversalToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return traversalToken{}, invalid
	}
	switch {
	case token.Plan != signature, token.Listing < 0, token.Listing >= listings,
		token.Pass != 1 && token.Pass != 2, token.Page < 1, token.Page > traktMaxPages,
		token.Skip < 0, (token.Skip > 0) != (token.PageSum != ""), token.Skip > 0 && token.Pass != 1,
		token.Count < 0, token.Pass == 2 && token.First == "", token.Attempt < 0, token.Attempt >= traktReadAttempts:
		return traversalToken{}, invalid
	}
	return token, nil
}

func encodeTraversal(token traversalToken) string {
	encoded, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func (s *Server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	plan, signature, fault := traversalPlan(req.GetStateKinds())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	token, fault := decodeTraversal(req.GetPageToken(), signature, len(plan))
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	ctx, cancel, stopAt := s.window(ctx)
	defer cancel()
	t := &traversal{client: client, plan: plan, token: token, stopAt: stopAt}
	return t.run(ctx), nil
}

type traversal struct {
	client *apiClient
	plan   []listing
	token  traversalToken
	stopAt time.Time
	// reads and bytes count this call's upstream reads and response size.
	reads int
	bytes int
	items []*pluginv1.WatchSyncRemoteState
}

type listingOutcome int

const (
	listingDone listingOutcome = iota
	listingPaused
	listingChanged
)

func (t *traversal) run(ctx context.Context) *pluginv1.WatchSyncListRemoteStateResponse {
	server := t.client.server
	for t.token.Listing < len(t.plan) {
		l := t.plan[t.token.Listing]
		restartable := t.token.unreturned()
		mark, markBytes := len(t.items), t.bytes
		outcome, fault := t.readListing(ctx, l)
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}
		}
		switch outcome {
		case listingPaused:
			return &pluginv1.WatchSyncListRemoteStateResponse{
				Items:            t.items,
				NextPageToken:    encodeTraversal(t.token),
				CompleteSnapshot: true,
			}
		case listingChanged:
			attempt := t.token.Attempt + 1
			if !restartable || attempt >= traktReadAttempts {
				return &pluginv1.WatchSyncListRemoteStateResponse{Fault: temporaryFault(
					"The Trakt " + l.name + " list changed while it was read; the next sync reads it again")}
			}
			t.items, t.bytes = t.items[:mark], markBytes
			t.token.listingRead = freshRead(attempt)
			if err := server.sleep(ctx, time.Duration(attempt)*time.Second); err != nil {
				return &pluginv1.WatchSyncListRemoteStateResponse{Fault: deadlineFault()}
			}
		default:
			t.token.Listing++
			t.token.listingRead = freshRead(0)
		}
	}
	// Every traversal is a full read, so each one is a complete snapshot and
	// no cursor is kept.
	return &pluginv1.WatchSyncListRemoteStateResponse{Items: t.items, CompleteSnapshot: true}
}

// readListing continues the current listing's read until it ends, changes,
// or this call's budget runs out.
func (t *traversal) readListing(ctx context.Context, l listing) (listingOutcome, *pluginv1.WatchSyncFault) {
	server := t.client.server
	read := &t.token.listingRead
	for {
		// Every call reads at least one page, so each page token moves on.
		if t.reads > 0 && (t.bytes >= server.responseBytes || !server.clock().Before(t.stopAt)) {
			return listingPaused, nil
		}
		page := read.Page
		rows, header, refused, fault := t.client.readPage(ctx, t.stopAt, l, page)
		if fault != nil {
			if refused && t.reads > 0 {
				return listingPaused, nil
			}
			return 0, fault
		}
		t.reads++
		if read.observeCount(header) || (read.Skip > 0 && rowsDigest(rows) != read.PageSum) {
			return listingChanged, nil
		}
		last := l.unpaged || lastTraktPage(header, page, len(rows))
		if read.Pass == 1 {
			for index := read.Skip; index < len(rows); index++ {
				states, err := l.states(rows[index])
				if err != nil {
					return 0, temporaryFault("Trakt returned an unreadable " + l.name + " list")
				}
				size := statesSize(states)
				if len(t.items) > 0 && t.bytes+size > server.responseBytes {
					// The page does not fit: return the rest of it next call.
					if index > 0 {
						if read.Skip == 0 {
							read.PageSum = rowsDigest(rows)
						}
						read.Skip = index
					}
					return listingPaused, nil
				}
				t.items = append(t.items, states...)
				t.bytes += size
				read.First = chainDigest(read.First, rows[index])
			}
			read.Skip, read.PageSum = 0, ""
			if last {
				if page == 1 {
					return listingDone, nil
				}
				read.Pass, read.Page, read.Count = 2, 1, 0
				continue
			}
		} else {
			for _, row := range rows {
				read.Second = chainDigest(read.Second, row)
			}
			if last {
				if read.Second != read.First {
					return listingChanged, nil
				}
				return listingDone, nil
			}
		}
		if page >= traktMaxPages {
			return 0, temporaryFault(fmt.Sprintf("The Trakt %s list did not end within %d pages", l.name, traktMaxPages))
		}
		read.Page++
	}
}

// observeCount records the page's X-Pagination-Item-Count and reports whether
// it differs from the count earlier pages of this pass reported.
func (r *listingRead) observeCount(header http.Header) bool {
	count, ok := positiveHeaderInt(header, "X-Pagination-Item-Count")
	if !ok {
		return false
	}
	changed := r.Count != 0 && count != r.Count
	r.Count = count
	return changed
}

// readPage reads one page of a listing. Paged reads wait on the per-token
// page limiter first; refused reports that the wait would run past stopAt, so
// the request was not sent.
func (c *apiClient) readPage(ctx context.Context, stopAt time.Time, l listing, page int) ([]json.RawMessage, http.Header, bool, *pluginv1.WatchSyncFault) {
	query := url.Values{}
	maps.Copy(query, l.query)
	if !l.unpaged {
		// Trakt serves only a short first page when page and limit are
		// omitted, so both are always sent.
		query.Set("limit", strconv.Itoa(traktPageLimit))
		query.Set("page", strconv.Itoa(page))
		waitCtx, cancel := context.WithDeadline(ctx, stopAt)
		err := c.server.pages.Wait(waitCtx, c.token)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, false, deadlineFault()
			}
			return nil, nil, true, rateLimitedFault(pageInterval)
		}
	}
	var rows []json.RawMessage
	_, header, fault := c.do(ctx, http.MethodGet, l.path, query, nil, &rows)
	if fault != nil {
		return nil, nil, false, fault
	}
	return rows, header, false, nil
}

// lastTraktPage reports whether page, holding items rows, ends the listing.
// X-Pagination-Page-Count is authoritative when present. Otherwise a page
// shorter than the applied X-Pagination-Limit is the last one. The requested
// limit is not a safe comparison: Trakt can apply a smaller one, particularly
// for shows with season progress, so without headers only an empty page ends
// the listing.
func lastTraktPage(header http.Header, page, items int) bool {
	if items == 0 {
		return true
	}
	if count, ok := positiveHeaderInt(header, "X-Pagination-Page-Count"); ok {
		return page >= count
	}
	if limit, ok := positiveHeaderInt(header, "X-Pagination-Limit"); ok {
		return items < limit
	}
	return false
}

func positiveHeaderInt(header http.Header, key string) (int, bool) {
	value, err := strconv.Atoi(strings.TrimSpace(header.Get(key)))
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

// chainDigest extends a running digest with one row, length-prefixed so row
// boundaries count.
func chainDigest(previous string, row json.RawMessage) string {
	hash := sha256.New()
	hash.Write([]byte(previous))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(row)))
	hash.Write(length[:])
	hash.Write(row)
	return hex.EncodeToString(hash.Sum(nil))
}

func rowsDigest(rows []json.RawMessage) string {
	digest := "page"
	for _, row := range rows {
		digest = chainDigest(digest, row)
	}
	return digest
}

func statesSize(states []*pluginv1.WatchSyncRemoteState) int {
	size := 0
	for _, state := range states {
		// The repeated field adds a tag and a length prefix to each item.
		size += proto.Size(state) + 6
	}
	return size
}

func watchedMovieStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktWatchedMovie
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return []*pluginv1.WatchSyncRemoteState{{
		ProviderItemKey: movieKey(row.Movie.IDs),
		Media:           movieMedia(row.Movie),
		Watched:         watchedState(row.Plays, row.LastWatchedAt),
	}}, nil
}

// watchedShowStates expands a watched show into one state per episode. The
// rows carry no episode ids, so episodes are keyed by show ids and numbers.
func watchedShowStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktWatchedShow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	var states []*pluginv1.WatchSyncRemoteState
	for _, season := range row.Seasons {
		for _, episode := range season.Episodes {
			states = append(states, &pluginv1.WatchSyncRemoteState{
				ProviderItemKey: episodeKey(row.Show.IDs, season.Number, episode.Number, traktIDs{}),
				Media: &pluginv1.WatchSyncMedia{
					MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
					SeriesTitle:       row.Show.Title,
					SeriesYear:        int32(row.Show.Year),
					SeriesExternalIds: externalIDs(row.Show.IDs),
					SeasonNumber:      int32(season.Number),
					EpisodeNumber:     int32(episode.Number),
				},
				Watched: watchedState(episode.Plays, episode.LastWatchedAt),
			})
		}
	}
	return states, nil
}

// watchedState reports at least one play: the host drops watched state
// without plays, and every title on Trakt's watched list has been played.
func watchedState(plays int, lastWatchedAt time.Time) *pluginv1.WatchSyncRemoteWatchedState {
	return &pluginv1.WatchSyncRemoteWatchedState{
		PlayCount:     int32(max(1, plays)),
		LastWatchedAt: timestamppb.New(lastWatchedAt),
	}
}

func playbackStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktPlayback
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	progress := &pluginv1.WatchSyncRemoteProgressState{
		ProgressPercent: min(max(row.Progress, 0), maxProgressPercent),
		PausedAt:        timestamppb.New(row.PausedAt),
	}
	switch row.Type {
	case "movie":
		return []*pluginv1.WatchSyncRemoteState{{
			ProviderItemKey: movieKey(row.Movie.IDs),
			Media:           movieMedia(row.Movie),
			Progress:        progress,
		}}, nil
	case "episode":
		return []*pluginv1.WatchSyncRemoteState{{
			ProviderItemKey: episodeKey(row.Show.IDs, row.Episode.Season, row.Episode.Number, row.Episode.IDs),
			Media:           episodeMedia(row.Show, row.Episode),
			Progress:        progress,
		}}, nil
	default:
		return nil, nil
	}
}

type listKind int

const (
	favoriteList listKind = iota
	watchlistList
)

func listedState(list listKind, listedAt time.Time, key string, media *pluginv1.WatchSyncMedia) *pluginv1.WatchSyncRemoteState {
	state := &pluginv1.WatchSyncRemoteState{ProviderItemKey: key, Media: media}
	listed := &pluginv1.WatchSyncRemoteListState{ListedAt: timestamppb.New(listedAt)}
	if list == favoriteList {
		state.Favorite = listed
	} else {
		state.Watchlist = listed
	}
	return state
}

func listedMovieStates(list listKind) func(json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	return func(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
		var row traktListedMovie
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		return []*pluginv1.WatchSyncRemoteState{listedState(list, row.ListedAt, movieKey(row.Movie.IDs), movieMedia(row.Movie))}, nil
	}
}

func listedShowStates(list listKind) func(json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	return func(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
		var row traktListedShow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		return []*pluginv1.WatchSyncRemoteState{listedState(list, row.ListedAt, showKey(row.Show.IDs), showMedia(row.Show))}, nil
	}
}

func movieMedia(movie traktMovie) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		Title:       movie.Title,
		Year:        int32(movie.Year),
		ExternalIds: externalIDs(movie.IDs),
	}
}

func showMedia(show traktShow) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
		Title:       show.Title,
		Year:        int32(show.Year),
		ExternalIds: externalIDs(show.IDs),
	}
}

func episodeMedia(show traktShow, episode traktEpisode) *pluginv1.WatchSyncMedia {
	return &pluginv1.WatchSyncMedia{
		MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		Title:             episode.Title,
		Year:              int32(episode.Year),
		ExternalIds:       externalIDs(episode.IDs),
		SeriesTitle:       show.Title,
		SeriesYear:        int32(show.Year),
		SeriesExternalIds: externalIDs(show.IDs),
		SeasonNumber:      int32(episode.Season),
		EpisodeNumber:     int32(episode.Number),
	}
}
