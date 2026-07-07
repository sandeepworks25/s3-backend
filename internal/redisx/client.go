package redisx

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb      *redis.Client
	logger   *slog.Logger
	mu       sync.RWMutex
	fallback map[string]fallbackItem
}

type fallbackItem struct {
	value     string
	expiresAt time.Time
}

func Open(addr string, logger *slog.Logger) *Client {
	options, err := redis.ParseURL(addr)
	if err != nil || !strings.Contains(addr, "://") {
		options = &redis.Options{Addr: addr}
	}
	rdb := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Warn("redis unavailable, cache operations will be best effort", "error", err)
	}
	return &Client{rdb: rdb, logger: logger, fallback: map[string]fallbackItem{}}
}

func (c *Client) Close() {
	_ = c.rdb.Close()
}

func (c *Client) Health(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	if err := c.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		c.logger.Debug("redis set failed", "key", key, "error", err)
		c.mu.Lock()
		c.fallback[key] = fallbackItem{value: value, expiresAt: time.Now().Add(ttl)}
		c.mu.Unlock()
	}
}

func (c *Client) Get(ctx context.Context, key string) (string, bool) {
	value, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		c.mu.RLock()
		item, ok := c.fallback[key]
		c.mu.RUnlock()
		if !ok || time.Now().After(item.expiresAt) {
			if ok {
				c.mu.Lock()
				delete(c.fallback, key)
				c.mu.Unlock()
			}
			return "", false
		}
		return item.value, true
	}
	return value, true
}

func (c *Client) Del(ctx context.Context, key string) {
	_ = c.rdb.Del(ctx, key).Err()
	c.mu.Lock()
	delete(c.fallback, key)
	c.mu.Unlock()
}
