package provider

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Provider item keys are stored by the host for every synced item. They must
// stay byte-identical to the keys Silo's built-in Trakt provider produced, so
// connections migrated from it keep their per-item rows.

// ID schemes, as used in provider item keys ("tmdb:949") and traktIDRef.
const (
	idSchemeTrakt = "trakt"
	idSchemeSlug  = "slug"
	idSchemeIMDb  = "imdb"
	idSchemeTMDB  = "tmdb"
	idSchemeTVDB  = "tvdb"
)

// Silo item kinds, used to namespace ids: TMDB and TVDB number movies, shows,
// and episodes independently.
const (
	kindMovie   = "movie"
	kindEpisode = "episode"
	kindSeries  = "series"
)

func movieKey(ids traktIDs) string {
	switch {
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(ids.TMDB)
	case ids.Trakt > 0:
		return "trakt:" + strconv.Itoa(ids.Trakt)
	default:
		return ""
	}
}

func showKey(ids traktIDs) string {
	switch {
	case ids.TVDB > 0:
		return "tvdb:" + strconv.Itoa(ids.TVDB)
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(ids.TMDB)
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case ids.Trakt > 0:
		return "trakt:" + strconv.Itoa(ids.Trakt)
	default:
		return ""
	}
}

func episodeKey(showIDs traktIDs, season, episode int, episodeIDs traktIDs) string {
	switch {
	case episodeIDs.TVDB > 0:
		return "tvdb:" + strconv.Itoa(episodeIDs.TVDB)
	case episodeIDs.TMDB > 0:
		return "tmdb:" + strconv.Itoa(episodeIDs.TMDB)
	case episodeIDs.Trakt > 0:
		return "trakt:" + strconv.Itoa(episodeIDs.Trakt)
	case showIDs.TVDB > 0:
		return fmt.Sprintf("show:tvdb:%d:s%d:e%d", showIDs.TVDB, season, episode)
	case showIDs.TMDB > 0:
		return fmt.Sprintf("show:tmdb:%d:s%d:e%d", showIDs.TMDB, season, episode)
	case showIDs.IMDb != "":
		return fmt.Sprintf("show:imdb:%s:s%d:e%d", showIDs.IMDb, season, episode)
	default:
		return fmt.Sprintf("episode:s%d:e%d", season, episode)
	}
}

// localPlayKey is the key the host gives a local play (its
// providerItemKeyForLocalPlay), from the play's own and series ids as the
// host sent them. History de-duplication compares it with the key of each
// play Trakt returns.
func localPlayKey(kind string, own, series map[string]string, season, episode int32) string {
	if kind == kindEpisode {
		switch {
		case own["tvdb"] != "":
			return "tvdb:" + own["tvdb"]
		case own["tmdb"] != "":
			return "tmdb:" + own["tmdb"]
		case series["tvdb"] != "":
			return fmt.Sprintf("show:tvdb:%s:s%d:e%d", series["tvdb"], season, episode)
		case series["tmdb"] != "":
			return fmt.Sprintf("show:tmdb:%s:s%d:e%d", series["tmdb"], season, episode)
		case series["imdb"] != "":
			return fmt.Sprintf("show:imdb:%s:s%d:e%d", series["imdb"], season, episode)
		}
	}
	switch {
	case own["imdb"] != "":
		return "imdb:" + own["imdb"]
	case own["tmdb"] != "":
		return "tmdb:" + own["tmdb"]
	case own["tvdb"] != "":
		return "tvdb:" + own["tvdb"]
	default:
		return ""
	}
}

// playMatchKey identifies a play for history de-duplication: the item kind
// and key, and the watch time at Trakt's precision. Trakt stores watched_at to
// the minute. The kind keeps a movie's TMDB id apart from an episode's.
func playMatchKey(kind, itemKey string, watchedAt time.Time) string {
	return kind + "|" + itemKey + "|" + watchedAt.UTC().Truncate(time.Minute).Format(time.RFC3339)
}

func idsFromProviderItemKey(key string) traktIDs {
	prefix, value, ok := strings.Cut(key, ":")
	if !ok || value == "" {
		return traktIDs{}
	}
	switch prefix {
	case idSchemeIMDb:
		return traktIDs{IMDb: value}
	case idSchemeTMDB:
		return traktIDs{TMDB: parseInt(value)}
	case idSchemeTVDB:
		return traktIDs{TVDB: parseInt(value)}
	case idSchemeTrakt:
		return traktIDs{Trakt: parseInt(value)}
	default:
		return traktIDs{}
	}
}

func hasAnyID(ids traktIDs) bool {
	return ids.TVDB != 0 || ids.TMDB != 0 || ids.IMDb != ""
}

// sendableIDs reports whether ids can identify a title in a Trakt sync write.
// Trakt accepts its own id as well as IMDb, TMDB, and TVDB ids.
func sendableIDs(ids traktIDs) bool {
	return hasAnyID(ids) || ids.Trakt > 0
}

// traktIDIndex matches items Trakt echoes back in a response, such as its
// not_found lists, to the request items that produced them. An echo matches an
// item when the two share ANY identifier (Trakt id, slug, IMDb, TMDB, or TVDB):
// Trakt may echo a different id subset than Silo sent. Identifiers are
// namespaced by item kind, and zero ids never match.
//
// An echo that carries only identifiers the item lacks (for example a bare
// Trakt id for an item known only by IMDb) cannot be matched, so callers treat
// that item as accepted.
type traktIDIndex map[traktIDRef]struct{}

type traktIDRef struct {
	kind   string
	scheme string
	value  string
}

func (idx traktIDIndex) add(kind string, ids traktIDs) {
	for _, ref := range traktIDRefs(kind, ids) {
		idx[ref] = struct{}{}
	}
}

func (idx traktIDIndex) matches(kind string, ids traktIDs) bool {
	for _, ref := range traktIDRefs(kind, ids) {
		if _, ok := idx[ref]; ok {
			return true
		}
	}
	return false
}

func traktIDRefs(kind string, ids traktIDs) []traktIDRef {
	refs := make([]traktIDRef, 0, 5)
	if ids.Trakt > 0 {
		refs = append(refs, traktIDRef{kind: kind, scheme: idSchemeTrakt, value: strconv.Itoa(ids.Trakt)})
	}
	if ids.Slug != "" {
		refs = append(refs, traktIDRef{kind: kind, scheme: idSchemeSlug, value: ids.Slug})
	}
	if ids.IMDb != "" {
		refs = append(refs, traktIDRef{kind: kind, scheme: idSchemeIMDb, value: ids.IMDb})
	}
	if ids.TMDB > 0 {
		refs = append(refs, traktIDRef{kind: kind, scheme: idSchemeTMDB, value: strconv.Itoa(ids.TMDB)})
	}
	if ids.TVDB > 0 {
		refs = append(refs, traktIDRef{kind: kind, scheme: idSchemeTVDB, value: strconv.Itoa(ids.TVDB)})
	}
	return refs
}

func intString(value int) string {
	if value == 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}
