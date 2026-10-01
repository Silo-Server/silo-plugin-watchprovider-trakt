package provider

import (
	"context"
	"net/http"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// applyScrobble forwards one playback event. Trakt itself decides from the
// progress whether a stop records a play (at 80% or more), as it did for
// Silo's built-in provider.
func (s *Server) applyScrobble(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	index := group.pending[0]
	event := group.events[index]
	if kind := mediaKind(event.GetMedia()); kind != kindMovie && kind != kindEpisode {
		group.reject(index, "Trakt scrobbles movies and episodes only")
		return nil
	}
	path := "/scrobble/start"
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		path = "/scrobble/pause"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		path = "/scrobble/stop"
	}
	status, _, fault := client.do(ctx, http.MethodPost, path, nil, scrobblePayload(event, s.clock()), nil)
	if fault != nil {
		// Trakt answers 409 when it scrobbled the same item moments ago, so a
		// redelivered stop does not record a second play.
		if status == http.StatusConflict {
			group.set(index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE)
			return nil
		}
		return group.fail([]int{index}, fault)
	}
	group.set(index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	return nil
}

// scrobblePayload names an episode by its own ids, or by its show's ids and
// its season and episode numbers when it has none.
func scrobblePayload(event *pluginv1.WatchSyncEvent, now time.Time) map[string]any {
	progress := 0.0
	if event.GetDurationSeconds() > 0 {
		progress = event.GetPositionSeconds() / event.GetDurationSeconds() * 100
	}
	payload := map[string]any{
		"progress":    progress,
		"app_version": "Silo",
		"app_date":    now.UTC().Format("2006-01-02"),
	}
	media := event.GetMedia()
	ids := traktIDsFrom(media.GetExternalIds())
	switch {
	case mediaKind(media) != kindEpisode:
		payload["movie"] = map[string]any{"ids": ids}
	case hasAnyID(ids):
		payload["episode"] = map[string]any{"ids": ids}
	default:
		payload["show"] = map[string]any{"ids": traktIDsFrom(media.GetSeriesExternalIds())}
		payload["episode"] = map[string]any{"season": media.GetSeasonNumber(), "number": media.GetEpisodeNumber()}
	}
	return payload
}
