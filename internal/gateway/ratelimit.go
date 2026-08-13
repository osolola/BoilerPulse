package gateway

import (
	"net"
	"net/http"
	"sync"
	"time"

	"boilerpulse/internal/api"
)

// RateLimitOptions configures the token-bucket rate limiter, per spec §52 /
// §67-A ("token-bucket rate limiting per API key/IP at the gateway").
type RateLimitOptions struct {
	RequestsPerSecond float64
	Burst             int
}

// DefaultRateLimitOptions returns a generous but real limit.
func DefaultRateLimitOptions() RateLimitOptions {
	return RateLimitOptions{RequestsPerSecond: 50, Burst: 100}
}

type bucket struct {
	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
}

// RateLimiter is a per-key (client IP) token bucket. Buckets refill
// continuously based on elapsed wall-clock time rather than on a ticker, so
// there's no background goroutine per client.
type RateLimiter struct {
	opts    RateLimitOptions
	mu      sync.Mutex
	buckets map[string]*bucket
}

// NewRateLimiter builds a limiter. A RequestsPerSecond of 0 or less
// disables rate limiting entirely (Allow always returns true) — useful for
// tests that don't want limiting noise.
func NewRateLimiter(opts RateLimitOptions) *RateLimiter {
	return &RateLimiter{opts: opts, buckets: make(map[string]*bucket)}
}

// Allow reports whether a request from key may proceed, consuming one
// token if so.
func (rl *RateLimiter) Allow(key string) bool {
	if rl.opts.RequestsPerSecond <= 0 {
		return true
	}

	rl.mu.Lock()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(rl.opts.Burst), lastFill: time.Now()}
		rl.buckets[key] = b
	}
	rl.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.tokens += now.Sub(b.lastFill).Seconds() * rl.opts.RequestsPerSecond
	if b.tokens > float64(rl.opts.Burst) {
		b.tokens = float64(rl.opts.Burst)
	}
	b.lastFill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// rateLimited wraps a handler with the gateway's rate limiter, keyed by
// client IP.
func (g *Gateway) rateLimited(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !g.limiter.Allow(clientKey(r)) {
			writeError(w, http.StatusTooManyRequests, api.ErrRateLimited, "rate limit exceeded, try again shortly")
			return
		}
		next(w, r)
	}
}

func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
