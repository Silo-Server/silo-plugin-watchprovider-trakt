package main

import (
	"reflect"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
)

func loadManifest(t *testing.T) *pluginv1.PluginManifest {
	t.Helper()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	return parsed
}

// The host registers the plugin under the provider key Silo's built-in Trakt
// provider used, so the identity must match it.
func TestManifestIdentifiesTheTraktProvider(t *testing.T) {
	t.Parallel()
	parsed := loadManifest(t)
	if parsed.GetPluginId() != "silo.watchprovider.trakt" || parsed.GetCategory() != "Watch Providers" {
		t.Fatalf("plugin = %q in %q", parsed.GetPluginId(), parsed.GetCategory())
	}
	capabilities := parsed.GetCapabilities()
	if len(capabilities) != 1 {
		t.Fatalf("capabilities = %d, want 1", len(capabilities))
	}
	capability := capabilities[0]
	if capability.GetType() != "watch_sync_provider.v1" || capability.GetId() != "trakt" || capability.GetDisplayName() != "Trakt" {
		t.Fatalf("capability = %q %q %q", capability.GetType(), capability.GetId(), capability.GetDisplayName())
	}
	if len(capability.GetConfigSchema()) != 0 {
		t.Fatalf("connection config = %v; device-code providers take none", capability.GetConfigSchema())
	}
}

// The descriptor advertises what the built-in provider did.
func TestManifestAdvertisesTheBuiltInCapabilities(t *testing.T) {
	t.Parallel()
	descriptor := loadManifest(t).GetCapabilities()[0].GetWatchSyncProvider()
	flags := map[string]bool{
		"import_watched":    descriptor.GetImportWatched(),
		"import_progress":   descriptor.GetImportProgress(),
		"export_watched":    descriptor.GetExportWatched(),
		"export_unwatched":  descriptor.GetExportUnwatched(),
		"import_favorites":  descriptor.GetImportFavorites(),
		"export_favorites":  descriptor.GetExportFavorites(),
		"remove_favorites":  descriptor.GetRemoveFavorites(),
		"import_watchlist":  descriptor.GetImportWatchlist(),
		"export_watchlist":  descriptor.GetExportWatchlist(),
		"remove_watchlist":  descriptor.GetRemoveWatchlist(),
		"scrobble_playback": descriptor.GetScrobblePlayback(),
		"import_ratings":    descriptor.GetImportRatings(),
		"export_ratings":    descriptor.GetExportRatings(),
		"sync_dropped":      descriptor.GetSyncDropped(),
	}
	for flag, set := range flags {
		if !set {
			t.Errorf("%s is not advertised", flag)
		}
	}
	// Rating a title on Trakt does not mark it watched, so no rating waits
	// for a play.
	if gated := descriptor.GetRatingExportRequiresWatched(); len(gated) != 0 {
		t.Errorf("rating_export_requires_watched = %v, want none", gated)
	}
	if descriptor.GetProvidesWatchlistOrder() {
		t.Error("provides_watchlist_order is advertised; Trakt's watchlist order is not synced")
	}
	if !reflect.DeepEqual(descriptor.GetAuthMethods(), []pluginv1.WatchSyncAuthMethod{pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE}) {
		t.Errorf("auth methods = %v, want device code", descriptor.GetAuthMethods())
	}
	wantMedia := []pluginv1.WatchSyncMediaType{
		pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
		pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
	}
	if !reflect.DeepEqual(descriptor.GetSupportedMediaTypes(), wantMedia) {
		t.Errorf("media types = %v, want %v", descriptor.GetSupportedMediaTypes(), wantMedia)
	}
	if !reflect.DeepEqual(descriptor.GetExternalIdNamespaces(), []string{"imdb", "tmdb", "tvdb"}) {
		t.Errorf("namespaces = %v", descriptor.GetExternalIdNamespaces())
	}
	if descriptor.GetMaxBatchSize() != 100 {
		t.Errorf("max batch size = %d, want 100", descriptor.GetMaxBatchSize())
	}
}

// The app credentials are install-wide: the client ID is public and the
// secret is kept on the host's protected path.
func TestManifestDeclaresTheTraktAppConfig(t *testing.T) {
	t.Parallel()
	schemas := loadManifest(t).GetGlobalConfigSchema()
	if len(schemas) != 1 || schemas[0].GetKey() != "app" || !schemas[0].GetRequired() {
		t.Fatalf("global config = %v", schemas)
	}
	fields := map[string]*pluginv1.AdminFormField{}
	for _, field := range schemas[0].GetAdminForm().GetFields() {
		fields[field.GetKey()] = field
	}
	if id := fields["client_id"]; id == nil || id.GetSecret() || id.GetControl() == pluginv1.AdminFormControl_ADMIN_FORM_CONTROL_PASSWORD || !id.GetRequired() {
		t.Fatalf("client_id field = %v", id)
	}
	if secret := fields["client_secret"]; secret == nil || !secret.GetSecret() || !secret.GetRequired() {
		t.Fatalf("client_secret field = %v", secret)
	}
	if len(fields) != 2 {
		t.Fatalf("fields = %v", fields)
	}
}
