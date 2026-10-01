package provider

import (
	"context"
	"net/http"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	capabilityID = "trakt"

	// Install-wide Trakt API app credentials, from the "app" global config.
	configClientID     = "app.client_id"
	configClientSecret = "app.client_secret"

	// An RPC stops starting upstream work after syncBudget, or earlier so that
	// its last request, bounded by defaultRequestTimeout, still ends
	// syncDeadlineMargin before the host's two-minute deadline.
	syncBudget         = 90 * time.Second
	syncDeadlineMargin = 10 * time.Second

	notConfiguredMessage = "Trakt is not set up on this server: an administrator must enter the Trakt app's client ID and client secret in the plugin settings"
	timeLimitMessage     = "Trakt sync time limit reached; the event will be retried"
)

// Server implements the watch-sync provider for Trakt. It keeps no state
// between RPCs except the in-memory request pacing, keyed by token digest.
type Server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	http      *http.Client
	baseURL   string
	userAgent string
	// writes paces authenticated writes per access token; pages paces paged
	// reads per access token.
	writes *credentialLimiter
	pages  *credentialLimiter
	// responseBytes bounds the encoded items of one ListRemoteState response.
	responseBytes int
	// sleep and now are replaced by tests.
	sleep func(context.Context, time.Duration) error
	now   func() time.Time
}

// NewServer returns a server that calls Trakt with httpClient, or a default
// client when nil. version is the plugin version sent in the User-Agent.
func NewServer(httpClient *http.Client, version string) *Server {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Server{
		http:      httpClient,
		baseURL:   defaultBaseURL,
		userAgent: userAgent(version),
		writes:    newCredentialLimiter(writeInterval, writeBurst),
		pages:     newCredentialLimiter(pageInterval, pageBurst),
		sleep:     sleepContext,
		// Well inside gRPC's 4 MiB message limit.
		responseBytes: 2 << 20,
		now:           time.Now,
	}
}

// DeviceAuthorization returns the device-code service that shares this
// server's HTTP client.
func (s *Server) DeviceAuthorization() pluginv1.WatchSyncDeviceAuthorizationServiceServer {
	return &deviceAuthorization{server: s}
}

// userAgent identifies the plugin to Trakt, which may block requests without
// an identifying User-Agent. Only HTTP token characters of the version are
// kept, so an unusual build version cannot invalidate requests.
func userAgent(version string) string {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	kept := strings.Map(func(r rune) rune {
		if r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return r
		}
		return -1
	}, version)
	if kept == "" {
		kept = "dev"
	}
	return "silo-plugin-watchprovider-trakt/" + kept
}

func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

type appConfig struct {
	clientID     string
	clientSecret string
}

// appConfigFrom reads the Trakt app credentials. The host sends the public
// client ID in values and the secret in secret_values; either map is accepted.
func appConfigFrom(config *pluginv1.WatchSyncProviderConfig) (appConfig, *pluginv1.WatchSyncFault) {
	lookup := func(key string) string {
		if value := strings.TrimSpace(config.GetValues()[key]); value != "" {
			return value
		}
		return strings.TrimSpace(config.GetSecretValues()[key])
	}
	app := appConfig{clientID: lookup(configClientID), clientSecret: lookup(configClientSecret)}
	if app.clientID == "" || app.clientSecret == "" {
		return appConfig{}, newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED, notConfiguredMessage)
	}
	return app, nil
}

// appClient returns a client for the unauthenticated OAuth endpoints.
func (s *Server) appClient(capability string, config *pluginv1.WatchSyncProviderConfig) (*apiClient, *pluginv1.WatchSyncFault) {
	if capability != capabilityID {
		return nil, invalidRequestFault("Unknown Trakt capability")
	}
	app, fault := appConfigFrom(config)
	if fault != nil {
		return nil, fault
	}
	return &apiClient{server: s, app: app}, nil
}

// authenticatedClient returns a client that signs requests with the
// connection's access token. Credentials of connections migrated from Silo's
// built-in provider carry only the tokens and their expiry.
func (s *Server) authenticatedClient(auth *pluginv1.WatchSyncAuthenticatedContext) (*apiClient, *pluginv1.WatchSyncFault) {
	client, fault := s.appClient(auth.GetCapabilityId(), auth.GetProviderConfig())
	if fault != nil {
		return nil, fault
	}
	client.token = strings.TrimSpace(auth.GetCredentials().GetAccessToken())
	if client.token == "" {
		return nil, invalidRequestFault("Trakt access token is required")
	}
	return client, nil
}

// window bounds one RPC: requests use the returned context, which ends
// syncDeadlineMargin before the host's deadline, and no upstream work starts
// after stopAt.
func (s *Server) window(ctx context.Context) (context.Context, context.CancelFunc, time.Time) {
	now := s.clock()
	stopAt := now.Add(syncBudget)
	if deadline, ok := ctx.Deadline(); ok {
		end := deadline.Add(-syncDeadlineMargin)
		if latest := end.Add(-defaultRequestTimeout); latest.Before(stopAt) {
			stopAt = latest
		}
		ctx, cancel := context.WithDeadline(ctx, end)
		return ctx, cancel, stopAt
	}
	ctx, cancel := context.WithTimeout(ctx, syncBudget+defaultRequestTimeout)
	return ctx, cancel, stopAt
}

func (s *Server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	ctx, cancel, _ := s.window(ctx)
	defer cancel()
	var settings traktUserSettings
	if _, _, fault := client.do(ctx, http.MethodGet, "/users/settings", nil, nil, &settings); fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	// The slug is the account id Silo's built-in provider stored, so existing
	// connections keep their account binding.
	id := settings.User.IDs.Slug
	if id == "" {
		id = settings.User.Username
	}
	if id == "" {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: temporaryFault("Trakt did not return the account's username")}, nil
	}
	displayName := settings.User.Name
	if displayName == "" {
		displayName = settings.User.Username
	}
	return &pluginv1.WatchSyncGetAccountResponse{Account: &pluginv1.WatchSyncAccount{
		ExternalSubject: id,
		Username:        settings.User.Username,
		DisplayName:     displayName,
	}}, nil
}

// RefreshCredentials exchanges the refresh token for new tokens. Trakt refresh
// tokens are single-use and rotate, so the response replaces both tokens.
func (s *Server) RefreshCredentials(ctx context.Context, req *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	auth := req.GetContext()
	client, fault := s.appClient(auth.GetCapabilityId(), auth.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	refreshToken := strings.TrimSpace(auth.GetCredentials().GetRefreshToken())
	if refreshToken == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			"The Trakt connection has no refresh token; reconnect Trakt")}, nil
	}
	ctx, cancel, _ := s.window(ctx)
	defer cancel()
	var token traktToken
	if _, _, fault := client.do(ctx, http.MethodPost, "/oauth/token", nil, map[string]string{
		"refresh_token": refreshToken,
		"client_id":     client.app.clientID,
		"client_secret": client.app.clientSecret,
		"grant_type":    "refresh_token",
	}, &token); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	credentials := s.credentials(token, refreshToken)
	if credentials == nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: temporaryFault("Trakt returned no access token")}, nil
	}
	return &pluginv1.WatchSyncCredentialResponse{Credentials: credentials}, nil
}

// credentials converts a token response into complete credentials, or nil
// when it holds no access token. A response without a refresh token keeps the
// previous one, as Silo's built-in provider did.
func (s *Server) credentials(token traktToken, previousRefresh string) *pluginv1.WatchSyncCredentials {
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil
	}
	credentials := &pluginv1.WatchSyncCredentials{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
	}
	if credentials.RefreshToken == "" {
		credentials.RefreshToken = previousRefresh
	}
	if token.ExpiresIn > 0 {
		credentials.ExpiresAt = timestamppb.New(s.clock().UTC().Add(time.Duration(token.ExpiresIn) * time.Second))
	}
	return credentials
}

func (s *Server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
	}
	ctx, cancel, stopAt := s.window(ctx)
	defer cancel()
	events := req.GetEvents()
	results := make([]*pluginv1.WatchSyncApplyResult, len(events))
	for start := 0; start < len(events); {
		end := groupEnd(events, start)
		if !s.clock().Before(stopAt) {
			for index := start; index < len(events); index++ {
				results[index] = resultFromFault(events[index].GetEventId(), temporaryFault(timeLimitMessage))
			}
			break
		}
		if fault := s.applyGroup(ctx, client, stopAt, events[start:end], results[start:end]); fault != nil {
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
		}
		start = end
	}
	for index, result := range results {
		if result == nil {
			results[index] = resultFromFault(events[index].GetEventId(), temporaryFault("Trakt sync did not reach this event; it will be retried"))
		}
	}
	return &pluginv1.WatchSyncApplyEventsResponse{Results: results}, nil
}

// groupEnd returns the end of the run of events starting at start that share
// one operation. Batched operations go to Trakt in one request; scrobbles and
// unknown operations go one at a time.
func groupEnd(events []*pluginv1.WatchSyncEvent, start int) int {
	operation := events[start].GetOperation()
	if !batched(operation) {
		return start + 1
	}
	end := start + 1
	for end < len(events) && events[end].GetOperation() == operation {
		end++
	}
	return end
}

func batched(operation pluginv1.WatchSyncOperation) bool {
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		return true
	default:
		return false
	}
}

// applyGroup applies events that share one operation and fills out, which is
// parallel to events. A fault that concerns the whole connection is returned
// instead.
func (s *Server) applyGroup(ctx context.Context, client *apiClient, stopAt time.Time, events []*pluginv1.WatchSyncEvent, out []*pluginv1.WatchSyncApplyResult) *pluginv1.WatchSyncFault {
	pending := make([]int, 0, len(events))
	for index, event := range events {
		if strings.TrimSpace(event.GetEventId()) == "" {
			out[index] = rejectedResult("", "Watch event ID is required")
			continue
		}
		if event.GetMedia() == nil {
			out[index] = rejectedResult(event.GetEventId(), "Watch event media is required")
			continue
		}
		pending = append(pending, index)
	}
	if len(pending) == 0 {
		return nil
	}
	group := eventGroup{events: events, out: out, pending: pending}
	switch operation := events[pending[0]].GetOperation(); operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		return s.applyWatched(ctx, client, stopAt, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED:
		return applyUnwatched(ctx, client, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST:
		return applyListEvents(ctx, client, operation, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		return applyRatingEvents(ctx, client, operation, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		return s.applyScrobble(ctx, client, operation, group)
	default:
		group.rejectAll("Trakt does not support this watch operation")
		return nil
	}
}

// eventGroup is a run of events with one operation. pending lists the
// indexes, into events and out, of the events still without a result.
type eventGroup struct {
	events  []*pluginv1.WatchSyncEvent
	out     []*pluginv1.WatchSyncApplyResult
	pending []int
}

func (g eventGroup) set(index int, status pluginv1.WatchSyncApplyStatus) {
	g.out[index] = &pluginv1.WatchSyncApplyResult{EventId: g.events[index].GetEventId(), Status: status}
}

func (g eventGroup) reject(index int, message string) {
	g.out[index] = rejectedResult(g.events[index].GetEventId(), message)
}

func (g eventGroup) rejectAll(message string) {
	for _, index := range g.pending {
		g.reject(index, message)
	}
}

// fail records a request failure for the given events, or returns the fault
// when it concerns the whole connection.
func (g eventGroup) fail(indexes []int, f *pluginv1.WatchSyncFault) *pluginv1.WatchSyncFault {
	if connectionWide(f) {
		return f
	}
	for _, index := range indexes {
		g.out[index] = resultFromFault(g.events[index].GetEventId(), f)
	}
	return nil
}

func resultFromFault(eventID string, f *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	if f.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
		f.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	}
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: status, Fault: f}
}

func rejectedResult(eventID, message string) *pluginv1.WatchSyncApplyResult {
	return resultFromFault(eventID, invalidRequestFault(message))
}

// mediaKind maps a media type to the item kind Silo's built-in provider used.
func mediaKind(media *pluginv1.WatchSyncMedia) string {
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		return kindMovie
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		return kindEpisode
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
		return kindSeries
	default:
		return ""
	}
}

// traktIDsFrom converts host external ids to Trakt ids.
func traktIDsFrom(ids map[string]string) traktIDs {
	return traktIDs{IMDb: ids["imdb"], TMDB: parseInt(ids["tmdb"]), TVDB: parseInt(ids["tvdb"])}
}

// externalIDs converts Trakt ids to the external id namespaces the host reads.
func externalIDs(ids traktIDs) map[string]string {
	out := make(map[string]string, 3)
	if ids.IMDb != "" {
		out["imdb"] = ids.IMDb
	}
	if value := intString(ids.TMDB); value != "" {
		out["tmdb"] = value
	}
	if value := intString(ids.TVDB); value != "" {
		out["tvdb"] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
