package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLock struct {
	client *redis.Client
	key    string
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

func (l *RedisLock) Acquire(ctx context.Context) (bool, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return false, err
	}
	l.token = hex.EncodeToString(buf)
	ok, err := l.client.SetNX(ctx, l.key, l.token, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (l *RedisLock) Refresh(ctx context.Context) (bool, error) {
	if l.token == "" {
		return false, nil
	}
	script := redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)
	res, err := script.Run(ctx, l.client, []string{l.key}, l.token, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (l *RedisLock) Release(ctx context.Context) error {
	if l.token == "" {
		return nil
	}
	_, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Result()
	return err
}
