package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	historyPath = "/sync/history"

	// historyWindowMaxPages bounds the history read that checks one window of
	// plays: 1,000 plays. A denser window is split in two.
	historyWindowMaxPages = 4

	traktTimeFormat = "2006-01-02T15:04:05.000Z"
)

// localPlay is a Silo play a MARK_WATCHED or MARK_UNWATCHED event carries.
type localPlay struct {
	index     int
	kind      string
	own       traktIDs
	series    traktIDs
	season    int
	episode   int
	watchedAt time.Time
	// matchKey is the play's kind, item key, and watch minute.
	matchKey string
}

func localPlayFrom(event *pluginv1.WatchSyncEvent, needTime bool) (localPlay, string) {
	media := event.GetMedia()
	play := localPlay{
		kind:    mediaKind(media),
		own:     traktIDsFrom(media.GetExternalIds()),
		series:  traktIDsFrom(media.GetSeriesExternalIds()),
		season:  int(media.GetSeasonNumber()),
		episode: int(media.GetEpisodeNumber()),
	}
	switch {
	case play.kind != kindMovie && play.kind != kindEpisode:
		return localPlay{}, "Trakt history holds movies and episodes only"
	case !hasAnyID(play.own) && (play.kind == kindMovie || !hasAnyID(play.series)):
		return localPlay{}, "Watch event needs an IMDb, TMDB, or TVDB ID"
	}
	if occurredAt := event.GetOccurredAt(); occurredAt != nil && occurredAt.CheckValid() == nil {
		play.watchedAt = occurredAt.AsTime().UTC()
	} else if needTime {
		return localPlay{}, "Watched event needs a valid watch time"
	}
	key := event.GetProviderItemKey()
	if key == "" {
		key = localPlayKey(play.kind, media.GetExternalIds(), media.GetSeriesExternalIds(), media.GetSeasonNumber(), media.GetEpisodeNumber())
	}
	play.matchKey = playMatchKey(play.kind, key, play.watchedAt)
	return play, ""
}

// applyWatched adds plays to the Trakt history. Trakt stores every play it is
// sent, including a repeat, so a play Trakt already has must not be sent
// again: neither a local play already on Trakt nor a retried event. Plays are
// matched as Silo's built-in provider matched them before export, by item key
// and watch minute, each remote play standing for at most one local play; a
// remote play is found under any key Silo could give its item.
func (s *Server) applyWatched(ctx context.Context, client *apiClient, stopAt time.Time, group eventGroup) *pluginv1.WatchSyncFault {
	var plays []localPlay
	// A repeated event ID is the same event; it shares the first one's result.
	first := make(map[string]int, len(group.pending))
	repeats := make(map[int]int)
	for _, index := range group.pending {
		eventID := group.events[index].GetEventId()
		if earlier, ok := first[eventID]; ok {
			repeats[index] = earlier
			continue
		}
		first[eventID] = index
		play, problem := localPlayFrom(group.events[index], true)
		if problem != "" {
			group.reject(index, problem)
			continue
		}
		play.index = index
		plays = append(plays, play)
	}
	if fault := s.addNewPlays(ctx, client, stopAt, group, plays); fault != nil {
		return fault
	}
	for index, earlier := range repeats {
		result := group.out[earlier]
		group.out[index] = &pluginv1.WatchSyncApplyResult{EventId: result.GetEventId(), Status: result.GetStatus(), Fault: result.GetFault()}
	}
	return nil
}

func (s *Server) addNewPlays(ctx context.Context, client *apiClient, stopAt time.Time, group eventGroup, plays []localPlay) *pluginv1.WatchSyncFault {
	if len(plays) == 0 {
		return nil
	}
	remote, unchecked, fault := s.remotePlays(ctx, client, stopAt, plays)
	if fault != nil {
		// A failed history read says nothing about the plays themselves, so
		// they stay retryable.
		if !connectionWide(fault) {
			fault = temporaryFault(fault.GetSafeMessage())
		}
		indexes := make([]int, 0, len(plays))
		for _, play := range plays {
			indexes = append(indexes, play.index)
		}
		return group.fail(indexes, fault)
	}
	var send []localPlay
	for _, play := range plays {
		switch {
		case unchecked[play.index] != nil:
			group.out[play.index] = resultFromFault(group.events[play.index].GetEventId(), unchecked[play.index])
		case remote[play.matchKey] > 0:
			remote[play.matchKey]--
			group.set(play.index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
		default:
			send = append(send, play)
		}
	}
	if len(send) == 0 {
		return nil
	}
	// Trakt reports titles it does not know in not_found; Silo's built-in
	// provider counted those plays as sent, and so does the plugin.
	if _, _, fault := client.do(ctx, http.MethodPost, historyPath, nil, historyPayload(send), nil); fault != nil {
		indexes := make([]int, 0, len(send))
		for _, play := range send {
			indexes = append(indexes, play.index)
		}
		return group.fail(indexes, fault)
	}
	for _, play := range send {
		group.set(play.index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	}
	return nil
}

// remotePlays counts the plays Trakt holds around the given plays, by match
// key. It reads the history between the earliest and the latest play, and
// splits the plays in two whenever that window holds more than
// historyWindowMaxPages pages, so a batch spread over years costs a few small
// reads instead of the whole history. unchecked holds the fault for each play
// whose window could not be read in this call.
func (s *Server) remotePlays(ctx context.Context, client *apiClient, stopAt time.Time, plays []localPlay) (map[string]int, map[int]*pluginv1.WatchSyncFault, *pluginv1.WatchSyncFault) {
	sorted := slices.Clone(plays)
	slices.SortStableFunc(sorted, func(a, b localPlay) int { return a.watchedAt.Compare(b.watchedAt) })
	remote := make(map[string]int)
	unchecked := make(map[int]*pluginv1.WatchSyncFault)
	// Windows can share a minute; a play Trakt returns twice counts once.
	counted := make(map[int64]bool)
	postpone := func(window []localPlay, f *pluginv1.WatchSyncFault) {
		for _, play := range window {
			unchecked[play.index] = f
		}
	}
	windows := [][]localPlay{sorted}
	for len(windows) > 0 {
		window := windows[len(windows)-1]
		windows = windows[:len(windows)-1]
		if !s.clock().Before(stopAt) {
			postpone(window, temporaryFault(timeLimitMessage))
			continue
		}
		from := window[0].watchedAt.Truncate(time.Minute).Add(-time.Minute)
		to := window[len(window)-1].watchedAt.Truncate(time.Minute).Add(2 * time.Minute)
		history := listing{name: "history", path: historyPath, query: url.Values{
			"start_at": {from.Format(traktTimeFormat)},
			"end_at":   {to.Format(traktTimeFormat)},
		}}
		rows, tooLarge, refused, fault := client.fetchAll(ctx, stopAt, history, historyWindowMaxPages)
		switch {
		case refused:
			// The page limiter would wait past this call's budget: defer like a
			// 429, which keeps the plays pending.
			postpone(window, rateLimitedFault(pageInterval))
			continue
		case fault != nil:
			return nil, nil, fault
		case tooLarge && len(window) == 1:
			postpone(window, temporaryFault("Trakt history around this play is too large to check"))
			continue
		case tooLarge:
			middle := len(window) / 2
			windows = append(windows, window[middle:], window[:middle])
			continue
		}
		for _, raw := range rows {
			var item traktHistoryItem
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, nil, temporaryFault("Trakt returned an unreadable history")
			}
			if item.ID != 0 {
				if counted[item.ID] {
					continue
				}
				counted[item.ID] = true
			}
			for _, key := range remoteItemKeys(item) {
				remote[playMatchKey(item.Type, key, item.WatchedAt)]++
			}
		}
	}
	return remote, unchecked, nil
}

// remoteItemKeys returns every key the host could give a played item. The
// host keys a local play by the ids Silo knows (providerItemKeyForLocalPlay),
// which may differ from the ids Trakt returns: a movie Silo knows only by TMDB
// is "tmdb:603" while Trakt's row also carries its IMDb id, and an episode
// Silo knows only by show and number is "show:tvdb:79126:s1:e2" while Trakt's
// row carries the episode's own ids. Only a play of the same item in the same
// minute can match, so each extra key is safe.
func remoteItemKeys(item traktHistoryItem) []string {
	var keys []string
	add := func(key string) {
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	switch item.Type {
	case kindMovie:
		ids := item.Movie.IDs
		if ids.IMDb != "" {
			add("imdb:" + ids.IMDb)
		}
		if ids.TMDB > 0 {
			add("tmdb:" + strconv.Itoa(ids.TMDB))
		}
		if ids.TVDB > 0 {
			add("tvdb:" + strconv.Itoa(ids.TVDB))
		}
	case kindEpisode:
		own, show := item.Episode.IDs, item.Show.IDs
		season, number := item.Episode.Season, item.Episode.Number
		if own.TVDB > 0 {
			add("tvdb:" + strconv.Itoa(own.TVDB))
		}
		if own.TMDB > 0 {
			add("tmdb:" + strconv.Itoa(own.TMDB))
		}
		if show.TVDB > 0 {
			add(fmt.Sprintf("show:tvdb:%d:s%d:e%d", show.TVDB, season, number))
		}
		if show.TMDB > 0 {
			add(fmt.Sprintf("show:tmdb:%d:s%d:e%d", show.TMDB, season, number))
		}
		if show.IMDb != "" {
			add(fmt.Sprintf("show:imdb:%s:s%d:e%d", show.IMDb, season, number))
		}
		if own.IMDb != "" {
			add("imdb:" + own.IMDb)
		}
	}
	return keys
}

// fetchAll reads every page of a listing inside this call, as Silo's built-in
// provider read paginated listings: a listing of several pages is read twice
// and must match, and an inconsistent read restarts a bounded number of
// times. tooLarge reports a listing of more than maxPages pages, which is not
// read further; refused reports that the page limiter would wait past stopAt.
func (c *apiClient) fetchAll(ctx context.Context, stopAt time.Time, l listing, maxPages int) (rows []json.RawMessage, tooLarge, refused bool, fault *pluginv1.WatchSyncFault) {
	for attempt := 1; ; attempt++ {
		first, pages, outcome, refused, fault := c.fetchPass(ctx, stopAt, l, maxPages)
		if fault != nil || outcome == passTooLarge {
			return nil, outcome == passTooLarge, refused, fault
		}
		if outcome == passComplete && pages > 1 {
			var again []json.RawMessage
			again, _, outcome, refused, fault = c.fetchPass(ctx, stopAt, l, maxPages)
			if fault != nil || outcome == passTooLarge {
				return nil, outcome == passTooLarge, refused, fault
			}
			if outcome == passComplete && !slices.EqualFunc(first, again, func(a, b json.RawMessage) bool { return bytes.Equal(a, b) }) {
				outcome = passChanged
			}
		}
		if outcome == passComplete {
			return first, false, false, nil
		}
		if attempt >= traktReadAttempts {
			return nil, false, false, temporaryFault("The Trakt " + l.name + " changed while it was read; the event will be retried")
		}
		if err := c.server.sleep(ctx, time.Duration(attempt)*time.Second); err != nil {
			return nil, false, false, deadlineFault()
		}
	}
}

type passOutcome int

const (
	passComplete passOutcome = iota
	passChanged
	passTooLarge
)

// fetchPass reads every page of a listing once and reports how many pages it
// took. A changed X-Pagination-Item-Count between pages fails the pass.
func (c *apiClient) fetchPass(ctx context.Context, stopAt time.Time, l listing, maxPages int) ([]json.RawMessage, int, passOutcome, bool, *pluginv1.WatchSyncFault) {
	var rows []json.RawMessage
	count := 0
	for page := 1; ; page++ {
		batch, header, refused, fault := c.readPage(ctx, stopAt, l, page)
		if fault != nil {
			return nil, 0, passComplete, refused, fault
		}
		if pages, ok := positiveHeaderInt(header, "X-Pagination-Page-Count"); ok && pages > maxPages {
			return nil, 0, passTooLarge, false, nil
		}
		if n, ok := positiveHeaderInt(header, "X-Pagination-Item-Count"); ok {
			if count != 0 && n != count {
				return nil, 0, passChanged, false, nil
			}
			count = n
		}
		rows = append(rows, batch...)
		if lastTraktPage(header, page, len(batch)) {
			return rows, page, passComplete, false, nil
		}
		if page >= maxPages {
			return nil, 0, passTooLarge, false, nil
		}
	}
}

// applyUnwatched removes plays from the Trakt history. Like Silo's built-in
// provider, a removal names the item without a watch time, so it clears every
// play of the item, and repeating it changes nothing.
func applyUnwatched(ctx context.Context, client *apiClient, group eventGroup) *pluginv1.WatchSyncFault {
	var plays []localPlay
	var indexes []int
	for _, index := range group.pending {
		play, problem := localPlayFrom(group.events[index], false)
		if problem != "" {
			group.reject(index, problem)
			continue
		}
		play.index = index
		plays = append(plays, play)
		indexes = append(indexes, index)
	}
	if len(plays) == 0 {
		return nil
	}
	if _, _, fault := client.do(ctx, http.MethodPost, historyPath+"/remove", nil, historyRemovePayload(plays), nil); fault != nil {
		return group.fail(indexes, fault)
	}
	for _, index := range indexes {
		group.set(index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	}
	return nil
}

func historyPayload(plays []localPlay) traktHistoryPayload {
	var payload traktHistoryPayload
	for _, play := range plays {
		watchedAt := play.watchedAt.UTC().Format(time.RFC3339)
		switch {
		case play.kind == kindMovie:
			payload.Movies = append(payload.Movies, traktHistoryMovie{WatchedAt: watchedAt, IDs: play.own})
		case hasAnyID(play.own):
			payload.Episodes = append(payload.Episodes, traktHistoryEpisode{WatchedAt: watchedAt, IDs: play.own})
		default:
			payload.Shows = appendNestedEpisode(payload.Shows, play.series, play.season, traktHistoryShowEpisode{
				Number:    play.episode,
				WatchedAt: watchedAt,
			})
		}
	}
	return payload
}

func historyRemovePayload(plays []localPlay) traktHistoryRemovePayload {
	var payload traktHistoryRemovePayload
	for _, play := range plays {
		switch {
		case play.kind == kindMovie:
			payload.Movies = append(payload.Movies, traktHistoryRemoveItem{IDs: play.own})
		case hasAnyID(play.own):
			payload.Episodes = append(payload.Episodes, traktHistoryRemoveItem{IDs: play.own})
		default:
			payload.Shows = appendNestedRemoveEpisode(payload.Shows, play.series, play.season, traktHistoryRemoveShowEpisode{
				Number: play.episode,
			})
		}
	}
	return payload
}

// appendNestedEpisode inserts an episode under the nested shows[] structure,
// merging by show ids then by season number so episodes of one show and
// season collapse into a single show and season entry.
func appendNestedEpisode(shows []traktHistoryShow, showIDs traktIDs, season int, episode traktHistoryShowEpisode) []traktHistoryShow {
	idx := slices.IndexFunc(shows, func(show traktHistoryShow) bool { return show.IDs == showIDs })
	if idx == -1 {
		shows = append(shows, traktHistoryShow{IDs: showIDs})
		idx = len(shows) - 1
	}
	seasons := &shows[idx].Seasons
	sIdx := slices.IndexFunc(*seasons, func(s traktHistorySeason) bool { return s.Number == season })
	if sIdx == -1 {
		*seasons = append(*seasons, traktHistorySeason{Number: season})
		sIdx = len(*seasons) - 1
	}
	(*seasons)[sIdx].Episodes = append((*seasons)[sIdx].Episodes, episode)
	return shows
}

func appendNestedRemoveEpisode(shows []traktHistoryRemoveShow, showIDs traktIDs, season int, episode traktHistoryRemoveShowEpisode) []traktHistoryRemoveShow {
	idx := slices.IndexFunc(shows, func(show traktHistoryRemoveShow) bool { return show.IDs == showIDs })
	if idx == -1 {
		shows = append(shows, traktHistoryRemoveShow{IDs: showIDs})
		idx = len(shows) - 1
	}
	seasons := &shows[idx].Seasons
	sIdx := slices.IndexFunc(*seasons, func(s traktHistoryRemoveSeason) bool { return s.Number == season })
	if sIdx == -1 {
		*seasons = append(*seasons, traktHistoryRemoveSeason{Number: season})
		sIdx = len(*seasons) - 1
	}
	(*seasons)[sIdx].Episodes = append((*seasons)[sIdx].Episodes, episode)
	return shows
}
