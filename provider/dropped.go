package provider

import (
	"encoding/json"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Trakt's "drop" hides a show from Up Next, progress, and the calendar without
// touching its history, and Trakt undrops a show on its own when the user
// watches it again. Dropped shows live in the hidden-items section "dropped";
// the older hidden sections (progress_watched, calendar) are not drops and
// are not read.
//
// The listing is read like every other one (see list_state.go), so a
// successful read is a complete snapshot, as Silo's built-in provider
// reported it. Trakt's listing leaves out drops that apps make, Silo's
// included. The host keeps a drop it sent as agreed until a read confirms it,
// so that omission neither undrops the show in Silo nor sends the drop again.
// applyListEvents writes drops and undrops, one request per event group.

// droppedShowStates converts one dropped row. Rows of another type are not
// drops and are skipped, as the built-in provider skipped them.
func droppedShowStates(raw json.RawMessage) ([]*pluginv1.WatchSyncRemoteState, error) {
	var row traktDroppedShow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	if row.Type != "" && row.Type != "show" {
		return nil, nil
	}
	dropped := &pluginv1.WatchSyncRemoteListState{}
	if !row.HiddenAt.IsZero() {
		dropped.ListedAt = timestamppb.New(row.HiddenAt)
	}
	return []*pluginv1.WatchSyncRemoteState{{
		ProviderItemKey: showKey(row.Show.IDs),
		Media:           showMedia(row.Show),
		Dropped:         dropped,
	}}, nil
}
