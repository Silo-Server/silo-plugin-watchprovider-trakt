package provider

import (
	"testing"
	"time"
)

// The keys must match Silo's built-in Trakt provider byte for byte, because
// migrated connections keep the per-item rows stored under them.
func TestProviderItemKeysMatchTheBuiltInProvider(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{"movie prefers imdb", movieKey(traktIDs{Trakt: 1, IMDb: "tt0113277", TMDB: 949}), "imdb:tt0113277"},
		{"movie falls back to tmdb", movieKey(traktIDs{Trakt: 1, TMDB: 949, TVDB: 5}), "tmdb:949"},
		{"movie falls back to trakt", movieKey(traktIDs{Trakt: 12601, TVDB: 5}), "trakt:12601"},
		{"movie without ids", movieKey(traktIDs{TVDB: 5}), ""},
		{"show prefers tvdb", showKey(traktIDs{Trakt: 2, IMDb: "tt0306414", TMDB: 1438, TVDB: 79126}), "tvdb:79126"},
		{"show falls back to tmdb", showKey(traktIDs{Trakt: 2, IMDb: "tt0306414", TMDB: 1438}), "tmdb:1438"},
		{"show falls back to imdb", showKey(traktIDs{Trakt: 2, IMDb: "tt0306414"}), "imdb:tt0306414"},
		{"show falls back to trakt", showKey(traktIDs{Trakt: 2}), "trakt:2"},
		{"episode prefers its tvdb", episodeKey(traktIDs{TVDB: 300}, 1, 2, traktIDs{Trakt: 9, TMDB: 8, TVDB: 501}), "tvdb:501"},
		{"episode falls back to its tmdb", episodeKey(traktIDs{TVDB: 300}, 1, 2, traktIDs{Trakt: 9, TMDB: 8}), "tmdb:8"},
		{"episode falls back to its trakt id", episodeKey(traktIDs{TVDB: 300}, 1, 2, traktIDs{Trakt: 9}), "trakt:9"},
		{"episode by show tvdb", episodeKey(traktIDs{TVDB: 789, TMDB: 1}, 0, 2, traktIDs{}), "show:tvdb:789:s0:e2"},
		{"episode by show tmdb", episodeKey(traktIDs{TMDB: 999, IMDb: "tt1"}, 2, 5, traktIDs{}), "show:tmdb:999:s2:e5"},
		{"episode by show imdb", episodeKey(traktIDs{IMDb: "tt4"}, 3, 4, traktIDs{}), "show:imdb:tt4:s3:e4"},
		{"episode without ids", episodeKey(traktIDs{Trakt: 3}, 3, 4, traktIDs{}), "episode:s3:e4"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: key = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

// localPlayKey mirrors the host's providerItemKeyForLocalPlay.
func TestLocalPlayKeyMatchesTheHostKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		kind   string
		own    map[string]string
		series map[string]string
		want   string
	}{
		{"movie imdb", kindMovie, map[string]string{"imdb": "tt1", "tmdb": "2", "tvdb": "3"}, nil, "imdb:tt1"},
		{"movie tmdb", kindMovie, map[string]string{"tmdb": "2", "tvdb": "3"}, nil, "tmdb:2"},
		{"movie tvdb", kindMovie, map[string]string{"tvdb": "3"}, nil, "tvdb:3"},
		{"episode own tvdb", kindEpisode, map[string]string{"imdb": "tt9", "tmdb": "8", "tvdb": "7"}, map[string]string{"tvdb": "1"}, "tvdb:7"},
		{"episode own tmdb", kindEpisode, map[string]string{"imdb": "tt9", "tmdb": "8"}, map[string]string{"tvdb": "1"}, "tmdb:8"},
		{"episode series tvdb", kindEpisode, map[string]string{"imdb": "tt9"}, map[string]string{"tvdb": "1", "tmdb": "2"}, "show:tvdb:1:s4:e5"},
		{"episode series tmdb", kindEpisode, nil, map[string]string{"tmdb": "2", "imdb": "tt3"}, "show:tmdb:2:s4:e5"},
		{"episode series imdb", kindEpisode, nil, map[string]string{"imdb": "tt3"}, "show:imdb:tt3:s4:e5"},
		{"episode own imdb only", kindEpisode, map[string]string{"imdb": "tt9"}, nil, "imdb:tt9"},
		{"nothing", kindEpisode, nil, nil, ""},
	} {
		if got := localPlayKey(tc.kind, tc.own, tc.series, 4, 5); got != tc.want {
			t.Errorf("%s: key = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestPlayMatchKeyComparesToTheMinute(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 5, 4, 12, 0, 59, 999, time.UTC)
	if got, want := playMatchKey(kindMovie, "tmdb:1", at), "movie|tmdb:1|2026-05-04T12:00:00Z"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
	other := time.Date(2026, 5, 4, 14, 0, 10, 0, time.FixedZone("CEST", 2*60*60))
	if playMatchKey(kindMovie, "tmdb:1", other) != playMatchKey(kindMovie, "tmdb:1", at) {
		t.Fatal("the same UTC minute in another zone must match")
	}
	if playMatchKey(kindEpisode, "tmdb:1", at) == playMatchKey(kindMovie, "tmdb:1", at) {
		t.Fatal("a movie and an episode with the same TMDB number must not match")
	}
}

func TestTraktIDIndexMatchesAnySharedIDPerKind(t *testing.T) {
	t.Parallel()
	idx := traktIDIndex{}
	idx.add(kindSeries, traktIDs{TVDB: 81189, Slug: "breaking-bad"})
	idx.add(kindMovie, traktIDs{})

	for _, tc := range []struct {
		name string
		kind string
		ids  traktIDs
		want bool
	}{
		{"shared tvdb", kindSeries, traktIDs{IMDb: "tt0903747", TVDB: 81189}, true},
		{"shared slug", kindSeries, traktIDs{Slug: "breaking-bad"}, true},
		{"other kind", kindEpisode, traktIDs{TVDB: 81189}, false},
		{"no shared id", kindSeries, traktIDs{IMDb: "tt0903747"}, false},
		{"zero ids never match", kindMovie, traktIDs{}, false},
	} {
		if got := idx.matches(tc.kind, tc.ids); got != tc.want {
			t.Errorf("%s: matches = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIDsFromProviderItemKey(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]traktIDs{
		"imdb:tt1":           {IMDb: "tt1"},
		"tmdb:949":           {TMDB: 949},
		"tvdb:79126":         {TVDB: 79126},
		"trakt:5":            {Trakt: 5},
		"show:tvdb:1:s1:e1":  {},
		"tmdb:":              {},
		"no-separator":       {},
		"episode:s1:e2":      {},
		"slug:breaking-bad":  {},
		"tmdb:not-a-number":  {},
		"imdb:tt1:extra:bit": {IMDb: "tt1:extra:bit"},
	} {
		if got := idsFromProviderItemKey(key); got != want {
			t.Errorf("idsFromProviderItemKey(%q) = %+v, want %+v", key, got, want)
		}
	}
}
