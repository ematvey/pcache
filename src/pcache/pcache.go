package pcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type RedisCache struct {
	Prefix string
	Pool   *redis.Pool
}

func RedisCacheNew(prefix, network, addr string) RedisCache {
	return RedisCache{
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

func CacheNew(prefix, network, addr string) Cache {
	return Cache(RedisCacheNew(prefix, network, addr))
}

func (c RedisCache) formatCacheKey(key string) string {
	return fmt.Sprintf("%+v.%+v", c.Prefix, key)
}

func (c RedisCache) restore(pulled, target interface{}) error {
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
func (c RedisCache) Set(key string, item interface{}, expire time.Duration) error {
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
func (c RedisCache) Get(key string, target interface{}) error {
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
func (c RedisCache) Ttl(key string) (ttl time.Duration, err error) {
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

func (c RedisCache) Proxy(
	key string,

	target interface{},
	fetcher func() (interface{}, error),

	expire time.Duration,
	refresh time.Duration,
	throttle time.Duration,
	timeout time.Duration,
) error {
	store := redisCacheBackend{pool: c.Pool, prefix: c.Prefix}
	locker := redisLocker{c.Pool}
	return proxy(
		store, locker,

		c.formatCacheKey(key),
		target,
		fetcher,
		expire,
		refresh,
		throttle,
		timeout,
	)
}
