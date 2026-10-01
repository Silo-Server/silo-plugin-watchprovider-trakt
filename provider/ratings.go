package provider

import (
	"context"
	"encoding/json"
	"net/http"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Trakt rates on the integer 1 to 10 scale the plugin contract uses, so
// ratings pass through unchanged. Only movie and show ratings are read and
// written: Silo rates movies and series, not seasons or episodes. Both rating
// listings are complete, so a title absent from them is unrated on Trakt.

func ratedMovieStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktRatedMovie
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return []*pluginv1.WatchSyncRemoteState{{
		ProviderItemKey: movieKey(row.Movie.IDs),
		Media:           movieMedia(row.Movie),
		Rating:          &pluginv1.WatchSyncRemoteRatingState{Rating: clampRating(row.Rating), RatedAt: timestamppb.New(row.RatedAt)},
	}}, nil
}

func ratedShowStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktRatedShow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return []*pluginv1.WatchSyncRemoteState{{
		ProviderItemKey: showKey(row.Show.IDs),
		Media:           showMedia(row.Show),
		Rating:          &pluginv1.WatchSyncRemoteRatingState{Rating: clampRating(row.Rating), RatedAt: timestamppb.New(row.RatedAt)},
	}}, nil
}

func clampRating(rating int) int32 {
	return int32(min(10, max(1, rating)))
}

// applyRatingEvents sets or clears movie and show ratings in one request.
// Trakt replaces an existing rating, so resending one is harmless, and it
// reports a title it does not know in not_found: a set for it is rejected,
// and a removal for it has nothing to clear.
func applyRatingEvents(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	remove := operation == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	var ratings traktRatingsPayload
	var removals traktIDList
	items := make([]listItem, 0, len(group.pending))
	for _, index := range group.pending {
		event := group.events[index]
		item, ok := listItemFrom(event)
		switch {
		case !ok || (item.kind != kindMovie && item.kind != kindSeries):
			group.reject(index, "Trakt syncs ratings for movies and shows only")
			continue
		case !sendableIDs(item.ids):
			group.reject(index, "Rating event needs an IMDb, TMDB, TVDB, or Trakt ID")
			continue
		case !remove && (event.GetRating() < 1 || event.GetRating() > 10):
			group.reject(index, "Rating must be an integer from 1 to 10")
			continue
		}
		item.index = index
		items = append(items, item)
		if remove {
			removals.add(item)
			continue
		}
		entry := traktRatingItem{Rating: int(event.GetRating()), IDs: item.ids}
		if occurredAt := event.GetOccurredAt(); occurredAt != nil && occurredAt.CheckValid() == nil && !occurredAt.AsTime().IsZero() {
			ratedAt := occurredAt.AsTime().UTC()
			entry.RatedAt = &ratedAt
		}
		if item.kind == kindMovie {
			ratings.Movies = append(ratings.Movies, entry)
		} else {
			ratings.Shows = append(ratings.Shows, entry)
		}
	}
	if len(items) == 0 {
		return nil
	}
	path, payload := "/sync/ratings", any(ratings)
	if remove {
		path, payload = "/sync/ratings/remove", removals
	}
	// The not_found lists echo {ids} (plus the rating on a set), the same
	// shape favorites use.
	var response traktNotFoundResponse
	if _, _, fault := client.do(ctx, http.MethodPost, path, nil, payload, &response); fault != nil {
		return group.fail(indexesOf(items), fault)
	}
	group.settle(items, response.NotFound, remove)
	return nil
}
