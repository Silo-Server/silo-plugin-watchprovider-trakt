package provider

import (
	"context"
	"net/http"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// listItem is a movie or show sent to a favorites, watchlist, dropped, or
// rating endpoint. index is its position in the event group.
type listItem struct {
	index int
	kind  string
	ids   traktIDs
}

// listItemFrom reads the ids an event's title is sent to Trakt with: its own
// external ids, falling back to the id its provider item key encodes.
func listItemFrom(event *pluginv1.WatchSyncEvent) (listItem, bool) {
	kind := mediaKind(event.GetMedia())
	if kind == "" {
		return listItem{}, false
	}
	ids := traktIDsFrom(event.GetMedia().GetExternalIds())
	if ids.IMDb == "" && ids.TMDB == 0 && ids.TVDB == 0 {
		ids = idsFromProviderItemKey(event.GetProviderItemKey())
	}
	return listItem{kind: kind, ids: ids}, true
}

func (l *traktIDList) add(item listItem) {
	if item.kind == kindMovie {
		l.Movies = append(l.Movies, traktIDItem{IDs: item.ids})
	} else {
		l.Shows = append(l.Shows, traktIDItem{IDs: item.ids})
	}
}

func indexesOf(items []listItem) []int {
	indexes := make([]int, 0, len(items))
	for _, item := range items {
		indexes = append(indexes, item.index)
	}
	return indexes
}

// listWrite is the Trakt endpoint for one list operation.
type listWrite struct {
	path   string
	remove bool
	// showsOnly is set for drops: Trakt drops shows, not movies.
	showsOnly bool
}

var listWrites = map[pluginv1.WatchSyncOperation]listWrite{
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE:          {path: "/sync/favorites"},
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE:       {path: "/sync/favorites/remove", remove: true},
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST:      {path: "/sync/watchlist"},
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST: {path: "/sync/watchlist/remove", remove: true},
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED:          {path: "/users/hidden/dropped", showsOnly: true},
	pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:        {path: "/users/hidden/dropped/remove", remove: true, showsOnly: true},
}

// accepts reports whether the list holds titles of kind.
func (w listWrite) accepts(kind string) bool {
	return kind == kindSeries || (kind == kindMovie && !w.showsOnly)
}

// applyListEvents adds titles to or removes them from favorites, the
// watchlist, or the dropped shows in one request. The endpoints accept and
// echo the same {movies, shows} id payload.
func applyListEvents(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	write := listWrites[operation]
	wrongKind := "Trakt favorites and watchlist hold movies and shows only"
	if write.showsOnly {
		wrongKind = "Trakt drops shows only"
	}
	var payload traktIDList
	items := make([]listItem, 0, len(group.pending))
	for _, index := range group.pending {
		item, ok := listItemFrom(group.events[index])
		switch {
		case !ok || !write.accepts(item.kind):
			group.reject(index, wrongKind)
			continue
		case !sendableIDs(item.ids):
			group.reject(index, "List event needs an IMDb, TMDB, TVDB, or Trakt ID")
			continue
		}
		item.index = index
		items = append(items, item)
		payload.add(item)
	}
	if len(items) == 0 {
		return nil
	}
	var response traktNotFoundResponse
	if _, _, fault := client.do(ctx, http.MethodPost, write.path, nil, payload, &response); fault != nil {
		return group.fail(indexesOf(items), fault)
	}
	group.settle(items, response.NotFound, write.remove)
	return nil
}

// settle maps Trakt's not_found echo onto the items sent. An item the echo
// names (see traktIDIndex) is unknown to Trakt: an add is rejected, and a
// removal has nothing to remove. Every other item was applied.
func (g eventGroup) settle(items []listItem, notFound traktIDList, remove bool) {
	missing := traktIDIndex{}
	for _, movie := range notFound.Movies {
		missing.add(kindMovie, movie.IDs)
	}
	for _, show := range notFound.Shows {
		missing.add(kindSeries, show.IDs)
	}
	for _, item := range items {
		switch {
		case !missing.matches(item.kind, item.ids):
			g.set(item.index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
		case remove:
			g.set(item.index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
		default:
			g.out[item.index] = resultFromFault(g.events[item.index].GetEventId(), permanentFault("Trakt could not find this title"))
		}
	}
}
