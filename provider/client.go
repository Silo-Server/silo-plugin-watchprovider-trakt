package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultBaseURL        = "https://api.trakt.tv"
	defaultRequestTimeout = 20 * time.Second

	// A page of 250 shows with season progress is the largest response the
	// plugin reads.
	maxResponseBytes = 32 << 20
	// maxErrorBody bounds how much of a failed response is read for its
	// OAuth error code.
	maxErrorBody = 4 << 10

	// statusAccountLimitExceeded is Trakt's 420, sent when a free account is
	// over an item limit that Trakt VIP raises. net/http has no constant.
	statusAccountLimitExceeded = 420

	// inPlaceRetryReserve is the time a retried request needs after its wait;
	// a wait that would not leave it before the deadline defers instead.
	inPlaceRetryReserve = 5 * time.Second
)

// apiClient sends the requests of one RPC: the install-wide app credentials
// and, after sign-in, the connection's access token.
type apiClient struct {
	server *Server
	app    appConfig
	token  string
}

// do sends one Trakt request and decodes a successful JSON response into out.
// Authenticated writes wait on the per-token write limiter, and a 429 with a
// short Retry-After is retried in place with the same body. The status is the
// final response's, or zero when none arrived.
func (c *apiClient) do(ctx context.Context, method, path string, query url.Values, payload, out any) (int, http.Header, *pluginv1.WatchSyncFault) {
	var body []byte
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, permanentFault("Trakt request could not be encoded")
		}
		body = encoded
	}
	// Trakt's write limit is per authenticated user. The OAuth endpoints are
	// unauthenticated and count against the application instead.
	paced := c.token != "" && method != http.MethodGet
	for attempt := 0; ; attempt++ {
		if paced {
			if err := c.server.writes.Wait(ctx, c.token); err != nil {
				return 0, nil, limiterWaitFault(ctx, writeInterval)
			}
		}
		result, fault := c.roundTrip(ctx, method, path, query, body, out)
		if fault != nil {
			return 0, nil, fault
		}
		if result.status != http.StatusTooManyRequests {
			if result.failed() {
				return result.status, result.header, responseFault(method, path, c.token != "", result)
			}
			return result.status, result.header, nil
		}
		wait := result.retryAfter()
		if attempt < maxRetryAttempts && wait <= maxInPlaceRetryWait && fitsBeforeDeadline(ctx, wait) {
			if err := c.server.sleep(ctx, wait); err != nil {
				return 0, nil, deadlineFault()
			}
			continue
		}
		// Repeated short hints that still end in 429 are not trustworthy, so
		// back off for a full fallback window rather than the last hint.
		if attempt >= maxRetryAttempts && wait < defaultRetryAfter {
			wait = defaultRetryAfter
		}
		return result.status, result.header, rateLimitedFault(wait)
	}
}

type httpResult struct {
	status int
	header http.Header
	// errorBody is the start of a failed response's body. It is parsed for an
	// OAuth error code and never shown.
	errorBody []byte
	now       time.Time
}

func (r httpResult) failed() bool {
	return r.status < http.StatusOK || r.status >= http.StatusMultipleChoices
}

// retryAfter is a 429's Retry-After, or defaultRetryAfter when the header is
// absent or malformed.
func (r httpResult) retryAfter() time.Duration {
	wait, ok := parseRetryAfter(r.header.Get("Retry-After"), r.now)
	if !ok {
		return defaultRetryAfter
	}
	return wait
}

// roundTrip performs a single HTTP attempt. A transport failure is a fault;
// any HTTP status is a result for the caller to map.
func (c *apiClient) roundTrip(ctx context.Context, method, path string, query url.Values, body []byte, out any) (httpResult, *pluginv1.WatchSyncFault) {
	endpoint := c.server.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return httpResult{}, permanentFault("Trakt request could not be created")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("trakt-api-version", "2")
	req.Header.Set("trakt-api-key", c.app.clientID)
	req.Header.Set("User-Agent", c.server.userAgent)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.server.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return httpResult{}, deadlineFault()
		}
		return httpResult{}, temporaryFault("Trakt is temporarily unreachable")
	}
	defer func() { _ = resp.Body.Close() }()
	result := httpResult{status: resp.StatusCode, header: resp.Header, now: c.server.clock()}
	if result.failed() {
		result.errorBody, _ = io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return result, nil
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(out); err != nil {
			if ctx.Err() != nil {
				return httpResult{}, deadlineFault()
			}
			return httpResult{}, temporaryFault("Trakt returned an unreadable response")
		}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	return result, nil
}

var oauthErrorCodePattern = regexp.MustCompile(`^[a-z_]{1,64}$`)

// oauthErrorCode returns the error code of a Trakt OAuth error body, such as
// {"error":"invalid_grant","error_description":"session not found"}. Only a
// well-formed code is returned; the free-text description never is.
func oauthErrorCode(body []byte) string {
	var oauth struct {
		Code string `json:"error"`
	}
	if json.Unmarshal(body, &oauth) != nil || !oauthErrorCodePattern.MatchString(oauth.Code) {
		return ""
	}
	return oauth.Code
}

var digitsPattern = regexp.MustCompile(`^[0-9]{1,12}$`)

// responseFault describes a failed Trakt response. A rejected access token
// (401 on a call that sent one) or refresh token (invalid_grant) is an invalid
// credential, so the profile owner is told to reconnect; refresh tokens issued
// before Trakt's 2026 authentication migration fail this way. The other account
// and app statuses say who can fix them.
func responseFault(method, path string, sentToken bool, result httpResult) *pluginv1.WatchSyncFault {
	failed := fmt.Sprintf("Trakt request %s %s failed (HTTP %d)", method, path, result.status)
	code := oauthErrorCode(result.errorBody)
	if code != "" {
		failed += " (" + code + ")"
	}
	switch {
	case code == "invalid_grant", result.status == http.StatusUnauthorized && sentToken:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			failed+": Trakt no longer accepts this connection's sign-in; reconnect Trakt")
	case result.status == http.StatusUnauthorized, result.status == http.StatusForbidden:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": Trakt rejected the server's app credentials; an administrator should check the Trakt client ID and secret in the plugin settings")
	case result.status == statusAccountLimitExceeded:
		if limit := strings.TrimSpace(result.header.Get("X-Account-Limit")); digitsPattern.MatchString(limit) {
			return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
				failed+": the Trakt account has reached its limit of "+limit+" items; Trakt VIP raises the limit")
		}
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": the Trakt account has reached an item limit; Trakt VIP raises the limit")
	case result.status == http.StatusLocked:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": the Trakt account is locked or deactivated; its owner should contact Trakt support")
	case result.status == http.StatusUpgradeRequired:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
			failed+": this Trakt feature needs Trakt VIP")
	case result.status == http.StatusNotFound:
		return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST,
			failed+": Trakt could not find the item")
	default:
		// Other refusals stay retryable: Silo's built-in provider recorded a
		// failed batch, which the host retries a few times, rather than items
		// Trakt does not know.
		return temporaryFault(failed)
	}
}

// fitsBeforeDeadline reports whether a wait still leaves a request time to
// finish before ctx's deadline.
func fitsBeforeDeadline(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) >= wait+inPlaceRetryReserve
}

// limiterWaitFault classifies a failed limiter wait. The limiter refuses when
// the next slot lies past ctx's deadline: the request was never sent, so the
// connection is deferred like a 429 instead of failing.
func limiterWaitFault(ctx context.Context, retryAfter time.Duration) *pluginv1.WatchSyncFault {
	if ctx.Err() != nil {
		return deadlineFault()
	}
	return rateLimitedFault(retryAfter)
}

func newFault(code pluginv1.WatchSyncFaultCode, message string) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{Code: code, SafeMessage: message}
}

func temporaryFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY, message)
}

func permanentFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMANENT, message)
}

func invalidRequestFault(message string) *pluginv1.WatchSyncFault {
	return newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_REQUEST, message)
}

func deadlineFault() *pluginv1.WatchSyncFault {
	return temporaryFault("Trakt did not answer before the sync deadline")
}

func rateLimitedFault(retryAfter time.Duration) *pluginv1.WatchSyncFault {
	return &pluginv1.WatchSyncFault{
		Code:        pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
		SafeMessage: "Trakt rate limit reached",
		RetryAfter:  durationpb.New(retryAfter),
	}
}

// connectionWide reports whether a fault concerns the whole connection rather
// than one event: it belongs on the ApplyEvents response.
func connectionWide(f *pluginv1.WatchSyncFault) bool {
	switch f.GetCode() {
	case pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED,
		pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED:
		return true
	default:
		return false
	}
}
