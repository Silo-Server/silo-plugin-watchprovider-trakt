# Trakt watch-provider plugin for Silo

Syncs Silo profiles with their [Trakt](https://trakt.tv) accounts through Silo's `watch_sync_provider.v1` plugin contract. It replaces the Trakt provider that was built into the Silo server, with the same behavior and the same stored identifiers, so existing connections carry over.

## Capabilities

- Signs each profile in with Trakt's device code: Silo shows a code, and the profile owner enters it at trakt.tv/activate.
- Imports watched movies and episodes with their play counts and last watch times.
- Imports resume progress for movies and episodes.
- Imports favorites, the watchlist, and movie and show ratings as complete lists, so a title removed on Trakt is removed in Silo.
- Exports completed plays. Trakt stores every play it receives, repeats included, so before writing a batch the plugin reads the Trakt history around those plays and skips any play Trakt already has in the same minute.
- Exports unwatched titles. Trakt removes history by title, so this clears every play of the title.
- Adds and removes favorites and watchlist entries, and sets and clears movie and show ratings. Trakt uses the same 1–10 rating scale as the plugin contract.
- Sends live playback start, pause, and stop events. Trakt records a play when a stop arrives at 80% progress or later.

### How the plugin reads Trakt

Every sync reads the full Trakt lists; Trakt has no change feed the plugin could resume from. Trakt pages its lists, and a list that changes while it is read can shift a row between pages. A list longer than one page is therefore read twice, and the read only counts when both passes match. A list that changes during a sync is read again up to three times; if it keeps changing, or it changes after the plugin has already returned part of it to Silo, that sync stops and the next one starts over.

The plugin keeps to Trakt's rate limits for each connection: one write per second, and paged reads paced to stay under 500 per five minutes. When Trakt asks it to wait ten seconds or less, the plugin waits and retries; a longer wait, or no stated wait (five minutes), pauses that profile's sync until then.

## Setup

1. Create an API app at [trakt.tv/oauth/applications/new](https://trakt.tv/oauth/applications/new). Trakt requires a redirect URI; enter `urn:ietf:wg:oauth:2.0:oob`. Device sign-in does not use it.
2. Install the plugin in Silo, open its settings, and enter the app's **Client ID** and **Client secret**. Every profile on the server connects through this one app.
3. Each profile connects its own Trakt account from its watch-provider settings.

Until the client ID and secret are set, every Trakt request fails with a message asking an administrator to configure the plugin.

## Upgrading from Silo's built-in Trakt provider

Existing Trakt connections move to the plugin once you run a Silo server release that serves Trakt through it. That release registers the plugin under the built-in provider's key, keeps every profile's sign-in and sync records, and copies the client ID and secret saved for the built-in provider into the plugin's settings. Profiles do not need to reconnect.

The first sync reads each account in full, as every Trakt sync does. Plays and ratings Silo already imported are not imported twice, and plays Silo already sent are not sent again.

A connection whose refresh token Trakt issued before its 2026 sign-in migration fails with "reconnect Trakt" under the plugin, as it did under the built-in provider. Signing in again fixes it.

## Not yet supported

- **Dropped shows.** The built-in provider synced shows dropped from Trakt's Up Next. The plugin contract does not express dropped shows yet; support follows in a later plugin release.

## Development

The plugin builds against `silo-plugin-sdk` v0.20.0.

```bash
make test
make build
./plugin manifest
```

`make build-all` produces static binaries for the platforms declared in `manifest.json`.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Changes to authentication, reconciliation, idempotency, or the watch-sync contract should start as an issue.

## License

AGPL-3.0-only.
