package pcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type RedisPCache struct {
	Prefix string
	Pool   *redis.Pool
}

func RedisPCacheNew(prefix, network, addr string) RedisPCache {
	return RedisPCache{
		prefix,
		&redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.Dial(network, addr)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

func (c RedisPCache) formatCacheKey(key string) string {
	return fmt.Sprintf("%+v.%+v", c.Prefix, key)
}

func (c RedisPCache) restore(pulled, target interface{}) error {
	if pulled != nil || target != nil {
		raw_bytes, ok := pulled.([]byte)
		if !ok {
			return errors.New(fmt.Sprintf("retrieved unexpected type: %+v", pulled))
		}
		return json.Unmarshal(raw_bytes, target)
	}
	return errors.New("incorrect args")
}

// Cache *item* as *key* with expiration *expire*
func (c RedisPCache) Set(key string, item interface{}, expire time.Duration) error {
	var conn = c.Pool.Get()
	defer conn.Close()
	cacheKey := c.formatCacheKey(key)
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	expS := expire.Seconds()
	if expS > 0 {
		_, err = conn.Do("SET", cacheKey, data, "EX", expire.Seconds())
	} else {
		_, err = conn.Do("SET", cacheKey, data)
	}
	return err
}

// Retrieve item under *key* and unmarshal it into *target*
func (c RedisPCache) Get(key string, target interface{}) error {
	var conn = c.Pool.Get()
	defer conn.Close()
	cacheKey := c.formatCacheKey(key)
	pulled, err := conn.Do("GET", cacheKey)
	if err != nil {
		return err
	}
	return c.restore(pulled, target)
}

// Check ttl of item under the *key*
func (c RedisPCache) Ttl(key string) (ttl time.Duration, err error) {
	var conn = c.Pool.Get()
	defer conn.Close()
	cacheKey := c.formatCacheKey(key)
	val, err := conn.Do("TTL", cacheKey)
	sec_ttl, ok := val.(int64)
	if !ok {
		err = errors.New("Ttl retrieved with incorrect type")
	}
	ttl = time.Duration(sec_ttl * int64(time.Second))
	return
}

func (c RedisPCache) Proxy(
	key string,

	target interface{},
	fetcher func() (interface{}, error),
	validator func(interface{}) bool,

	expire time.Duration,
	refresh time.Duration,
	throttle time.Duration,
	timeout time.Duration,
) error {
	store := redisKeyValueStore{pool: c.Pool, prefix: c.Prefix}
	locker := redisLocker{c.Pool}
	return ProxyCall(
		store, locker,

		c.formatCacheKey(key),
		target,
		fetcher,
		validator,

		expire,
		refresh,
		throttle,
		timeout,
	)
}
