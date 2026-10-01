package provider

import "time"

// traktIDs is Trakt's ids object. Its fields are not omitempty: Trakt accepts
// zero values, and the payloads match what Silo's built-in provider sent.
type traktIDs struct {
	Trakt int    `json:"trakt"`
	Slug  string `json:"slug"`
	IMDb  string `json:"imdb"`
	TMDB  int    `json:"tmdb"`
	TVDB  int    `json:"tvdb"`
}

type traktMovie struct {
	Title string   `json:"title"`
	Year  int      `json:"year"`
	IDs   traktIDs `json:"ids"`
}

type traktShow struct {
	Title string   `json:"title"`
	Year  int      `json:"year"`
	IDs   traktIDs `json:"ids"`
}

type traktEpisode struct {
	Title  string   `json:"title"`
	Year   int      `json:"year"`
	Season int      `json:"season"`
	Number int      `json:"number"`
	IDs    traktIDs `json:"ids"`
}

type traktWatchedMovie struct {
	Plays         int        `json:"plays"`
	LastWatchedAt time.Time  `json:"last_watched_at"`
	Movie         traktMovie `json:"movie"`
}

type traktWatchedShow struct {
	Show    traktShow `json:"show"`
	Seasons []struct {
		Number   int `json:"number"`
		Episodes []struct {
			Number        int       `json:"number"`
			Plays         int       `json:"plays"`
			LastWatchedAt time.Time `json:"last_watched_at"`
		} `json:"episodes"`
	} `json:"seasons"`
}

type traktPlayback struct {
	Type     string       `json:"type"`
	Progress float64      `json:"progress"`
	PausedAt time.Time    `json:"paused_at"`
	Movie    traktMovie   `json:"movie"`
	Show     traktShow    `json:"show"`
	Episode  traktEpisode `json:"episode"`
}

type traktHistoryItem struct {
	// ID is Trakt's id for the play, which no other play shares.
	ID        int64        `json:"id"`
	Type      string       `json:"type"`
	WatchedAt time.Time    `json:"watched_at"`
	Movie     traktMovie   `json:"movie"`
	Show      traktShow    `json:"show"`
	Episode   traktEpisode `json:"episode"`
}

// traktListedMovie and traktListedShow are rows of the favorites and watchlist
// listings, which share one shape.
type traktListedMovie struct {
	ListedAt time.Time  `json:"listed_at"`
	Movie    traktMovie `json:"movie"`
}

type traktListedShow struct {
	ListedAt time.Time `json:"listed_at"`
	Show     traktShow `json:"show"`
}

type traktRatedMovie struct {
	RatedAt time.Time  `json:"rated_at"`
	Rating  int        `json:"rating"`
	Movie   traktMovie `json:"movie"`
}

type traktRatedShow struct {
	RatedAt time.Time `json:"rated_at"`
	Rating  int       `json:"rating"`
	Show    traktShow `json:"show"`
}

// traktDroppedShow is a row of the dropped hidden-items listing.
type traktDroppedShow struct {
	HiddenAt time.Time `json:"hidden_at"`
	Type     string    `json:"type"`
	Show     traktShow `json:"show"`
}

type traktUserSettings struct {
	User struct {
		Username string `json:"username"`
		Name     string `json:"name"`
		IDs      struct {
			Slug string `json:"slug"`
		} `json:"ids"`
	} `json:"user"`
}

type traktDeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURL string `json:"verification_url"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type traktToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// History writes: a movie or an episode with its own id goes in the flat
// arrays; an episode without one is addressed through shows[] by show ids,
// season number, and episode number, the only shape Trakt accepts for it.
type traktHistoryPayload struct {
	Movies   []traktHistoryMovie   `json:"movies,omitempty"`
	Episodes []traktHistoryEpisode `json:"episodes,omitempty"`
	Shows    []traktHistoryShow    `json:"shows,omitempty"`
}

type traktHistoryMovie struct {
	WatchedAt string   `json:"watched_at"`
	IDs       traktIDs `json:"ids"`
}

type traktHistoryEpisode struct {
	WatchedAt string   `json:"watched_at"`
	IDs       traktIDs `json:"ids"`
}

type traktHistoryShow struct {
	IDs     traktIDs             `json:"ids"`
	Seasons []traktHistorySeason `json:"seasons"`
}

type traktHistorySeason struct {
	Number   int                       `json:"number"`
	Episodes []traktHistoryShowEpisode `json:"episodes"`
}

type traktHistoryShowEpisode struct {
	Number    int    `json:"number"`
	WatchedAt string `json:"watched_at"`
}

// The remove forms mirror the add forms without watched_at, so a removal
// clears every play of the item.
type traktHistoryRemovePayload struct {
	Movies   []traktHistoryRemoveItem `json:"movies,omitempty"`
	Episodes []traktHistoryRemoveItem `json:"episodes,omitempty"`
	Shows    []traktHistoryRemoveShow `json:"shows,omitempty"`
}

type traktHistoryRemoveItem struct {
	IDs traktIDs `json:"ids"`
}

type traktHistoryRemoveShow struct {
	IDs     traktIDs                   `json:"ids"`
	Seasons []traktHistoryRemoveSeason `json:"seasons"`
}

type traktHistoryRemoveSeason struct {
	Number   int                             `json:"number"`
	Episodes []traktHistoryRemoveShowEpisode `json:"episodes"`
}

type traktHistoryRemoveShowEpisode struct {
	Number int `json:"number"`
}

// traktIDList is the {movies, shows} id payload that favorites, the
// watchlist, dropped shows, and rating removal accept, and the shape of their
// not_found echo.
type traktIDList struct {
	Movies []traktIDItem `json:"movies,omitempty"`
	Shows  []traktIDItem `json:"shows,omitempty"`
}

type traktIDItem struct {
	IDs traktIDs `json:"ids"`
}

type traktNotFoundResponse struct {
	NotFound traktIDList `json:"not_found"`
}

type traktRatingsPayload struct {
	Movies []traktRatingItem `json:"movies,omitempty"`
	Shows  []traktRatingItem `json:"shows,omitempty"`
}

type traktRatingItem struct {
	Rating  int        `json:"rating"`
	RatedAt *time.Time `json:"rated_at,omitempty"`
	IDs     traktIDs   `json:"ids"`
}
