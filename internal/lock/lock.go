package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLock is a single-holder distributed lock backed by a Redis key with a
// random token. The token is guarded by a mutex because a lock instance can be
// refreshed from multiple goroutines (e.g. the coordinator's leader loop and
// its recovery loop), and Acquire previously overwrote the token even when
// acquisition failed, which both raced with concurrent readers and corrupted
// the "held token" for Release/Refresh.
type RedisLock struct {
	client *redis.Client
	key    string
	mu     sync.Mutex
	token  string
	ttl    time.Duration
}

func New(client *redis.Client, key string, ttl time.Duration) *RedisLock {
	return &RedisLock{client: client, key: key, ttl: ttl}
}

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
end
return 0
`)

var refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// Acquire attempts to take the lock. The member token is only updated when
// acquisition succeeds, so a failed attempt never clobbers a token that a
// previous successful acquire (or a concurrent Refresh) still relies on.
func (l *RedisLock) Acquire(ctx context.Context) (bool, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return false, err
	}
	token := hex.EncodeToString(buf)
	ok, err := l.client.SetNX(ctx, l.key, token, l.ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		l.mu.Lock()
		l.token = token
		l.mu.Unlock()
	}
	return ok, nil
}

// Refresh extends the TTL iff this instance still holds the lock.
func (l *RedisLock) Refresh(ctx context.Context) (bool, error) {
	l.mu.Lock()
	token := l.token
	l.mu.Unlock()
	if token == "" {
		return false, nil
	}
	res, err := refreshScript.Run(ctx, l.client, []string{l.key}, token, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// Release deletes the lock iff this instance holds it.
func (l *RedisLock) Release(ctx context.Context) error {
	l.mu.Lock()
	token := l.token
	l.mu.Unlock()
	if token == "" {
		return nil
	}
	_, err := releaseScript.Run(ctx, l.client, []string{l.key}, token).Result()
	return err
}
