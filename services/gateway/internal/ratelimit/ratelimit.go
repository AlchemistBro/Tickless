// Package ratelimit implements a distributed token bucket on Redis. The
// bucket state lives in one Redis hash and is updated by a Lua script, so the
// check is atomic across all gateway instances and uses Redis' own clock.
package ratelimit

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var script = redis.NewScript(`
local rate  = tonumber(ARGV[1])   -- tokens per second
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)   -- ms
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)
local allowed = 0
if tokens >= cost then tokens = tokens - cost; allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, tostring(tokens)}
`)

// Limiter checks buckets.
type Limiter struct{ rdb redis.Scripter }

// New returns a Limiter on rdb.
func New(rdb redis.Scripter) *Limiter { return &Limiter{rdb: rdb} }

// Result of an Allow call.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Allow takes cost tokens from the bucket key (rate tokens/s, capacity burst).
// err is non-nil when Redis is unreachable; callers decide whether to fail
// open (the gateway does, and counts it).
func (l *Limiter) Allow(ctx context.Context, key string, rate, burst float64, cost int) (Result, error) {
	res, err := script.Run(ctx, l.rdb, []string{key}, rate, burst, cost).Slice()
	if err != nil {
		return Result{Allowed: true}, err
	}
	allowed := res[0].(int64) == 1
	if allowed {
		return Result{Allowed: true}, nil
	}
	var tokens float64
	if s, ok := res[1].(string); ok {
		tokens, _ = strconv.ParseFloat(s, 64)
	}
	wait := (float64(cost) - tokens) / rate
	return Result{Allowed: false, RetryAfter: time.Duration(math.Ceil(wait*1000)) * time.Millisecond}, nil
}
