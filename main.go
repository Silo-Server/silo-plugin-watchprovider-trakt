package main

import (
	_ "embed"

	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/Silo-Server/silo-plugin-watchprovider-trakt/provider"
)

// version is set at build time with -ldflags "-X main.version=...".
var version string

//go:embed manifest.json
var manifestJSON []byte

func main() {
	server := provider.NewServer(nil, version)
	runtime.ServeManifestWithOptions(manifestJSON, version, runtime.CapabilityServers{
		WatchSyncProvider: server,
	}, runtime.WithWatchSyncDeviceAuthorization(server.DeviceAuthorization()))
}
