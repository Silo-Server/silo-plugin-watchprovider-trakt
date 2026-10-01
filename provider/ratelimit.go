package provider

import (
	"context"
	"crypto/sha256"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Trakt rate limits, from its API rate-limiting guide: authenticated users get
// one POST/PUT/DELETE per second (AUTHED_API_POST_LIMIT) and 500 GETs per five
// minutes (AUTHED_API_GET_LIMIT). Writes are paced to one per second. Paged
// reads, which a large history can stretch to hundreds of pages (read twice
// for consistency), are paced so any five-minute window stays inside the GET
// budget: a burst of 50 covers ordinary accounts at full speed, and the
// refill keeps burst plus five minutes of refill under 500.
const (
	writeInterval = time.Second
	writeBurst    = 1
	pageInterval  = 675 * time.Millisecond
	pageBurst     = 50

	// A 429 whose Retry-After is this short, which is typical of the
	// one-second write limit, is retried in place. Longer waits defer the
	// connection instead of holding an RPC open.
	maxInPlaceRetryWait = 10 * time.Second
	maxRetryAttempts    = 2

	// Trakt's limiter sends Retry-After, but 429s from its security layer may
	// not. Without a hint, wait out one full window of the longest documented
	// bucket (AUTHED_API_GET_LIMIT, 300 seconds) so whichever bucket tripped
	// has reset.
	defaultRetryAfter = 5 * time.Minute
)

// parseRetryAfter reads an RFC 9110 Retry-After value: delay-seconds or an
// HTTP-date. ok is false when the value is absent or malformed. An HTTP-date
// that has already passed parses as zero.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}

// sleepContext waits for d, returning early with ctx.Err() when ctx ends.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

const credentialLimiterIdleTTL = 10 * time.Minute

// credentialLimiter paces requests per credential, so one account's burst
// never delays another's. It lives in the plugin process and is keyed by a
// SHA-256 digest of the credential: the map never holds a raw token.
type credentialLimiter struct {
	every time.Duration
	burst int
	now   func() time.Time

	mu        sync.Mutex
	limiters  map[[sha256.Size]byte]*credentialLimiterEntry
	lastSweep time.Time
}

type credentialLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

func newCredentialLimiter(every time.Duration, burst int) *credentialLimiter {
	return &credentialLimiter{
		every:    every,
		burst:    max(burst, 1),
		now:      time.Now,
		limiters: make(map[[sha256.Size]byte]*credentialLimiterEntry),
	}
}

// Wait blocks until credential may send its next request. It returns an error
// without waiting when ctx is already done or its deadline would pass first.
func (l *credentialLimiter) Wait(ctx context.Context, credential string) error {
	return l.limiter(credential).Wait(ctx)
}

func (l *credentialLimiter) limiter(credential string) *rate.Limiter {
	key := sha256.Sum256([]byte(credential))
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= credentialLimiterIdleTTL {
		l.lastSweep = now
		// An idle limiter with a full bucket behaves exactly like a new one, so
		// dropping it cannot let a credential exceed its rate.
		for key, entry := range l.limiters {
			if now.Sub(entry.lastUsed) >= credentialLimiterIdleTTL &&
				entry.limiter.TokensAt(now) >= float64(l.burst) {
				delete(l.limiters, key)
			}
		}
	}
	entry, ok := l.limiters[key]
	if !ok {
		entry = &credentialLimiterEntry{limiter: rate.NewLimiter(rate.Every(l.every), l.burst)}
		l.limiters[key] = entry
	}
	entry.lastUsed = now
	return entry.limiter
}
