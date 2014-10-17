package pcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Redis-flavor of ProxyCache, that uses redis for both Store and Locker backends
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

func (c RedisPCache) Call(
	key string,

	target interface{},
	fetcher func() (interface{}, error),
	validator func(interface{}) bool,

	expire time.Duration,
	ttl time.Duration,
	throttle time.Duration,
	timeout time.Duration,
) error {
	spec := ResourceSpec{
		Locker:    redisLocker{c.Pool},
		Store:     redisKeyValueStore{pool: c.Pool, prefix: c.Prefix},
		Validator: validator,
		Expire:    expire,
		Ttl:       ttl,
		Throttle:  throttle,
		Timeout:   timeout,
	}
	return ProxyCache(c.formatCacheKey(key), target, fetcher, &spec)
}

type redisKeyValueStore struct {
	pool   *redis.Pool
	prefix string
}

func (r redisKeyValueStore) Get(
	key string, target interface{},
) (
	retrieved bool, creation_time *time.Time, last_fetch_time *time.Time,
) {
	var conn = r.pool.Get()
	defer conn.Close()
	cacheKey := key
	lrKey := cacheKey + ".lrt"
	crKey := cacheKey + ".crt"
	conn.Send("GET", cacheKey)
	conn.Send("GET", crKey)
	conn.Send("GET", lrKey)
	conn.Flush()
	item_raw, err_item := conn.Receive()
	crt_raw, err_crt := conn.Receive()
	lft_raw, err_lft := conn.Receive()
	if err_item == nil {
		restore := func(pulled, target interface{}) error {
			if pulled != nil || target != nil {
				raw_bytes, ok := pulled.([]byte)
				if !ok {
					return errors.New(fmt.Sprintf("retrieved unexpected type: %+v", pulled))
				}
				return json.Unmarshal(raw_bytes, target)
			}
			return errors.New("incorrect args")
		}
		err_restore := restore(item_raw, target)
		if err_restore == nil {
			retrieved = true
		}
	}
	if err_crt == nil && crt_raw != nil {
		t := parseRedisTime(crt_raw)
		creation_time = &t
	}
	if err_lft == nil && lft_raw != nil {
		t := parseRedisTime(lft_raw)
		last_fetch_time = &t
	}
	return
}
func (r redisKeyValueStore) Set(key string, item interface{}, expire time.Duration) error {
	var conn = r.pool.Get()
	defer conn.Close()
	cacheKey := key
	lrKey := cacheKey + ".lrt"
	crKey := cacheKey + ".crt"
	expire_seconds := expire.Seconds()
	bytes_raw, _ := json.Marshal(item)
	now_unix := time.Now().Unix()
	conn.Send("SET", cacheKey, bytes_raw, "EX", expire_seconds)
	conn.Send("SET", crKey, now_unix, "EX", expire_seconds)
	conn.Send("SET", lrKey, now_unix, "EX", expire_seconds)
	conn.Flush()
	return nil
}

type redisLocker struct {
	pool *redis.Pool
}

func (l redisLocker) IsLocked(key string) bool {
	var conn = l.pool.Get()
	defer conn.Close()
	lock_key := key + ".lock"
	lock, _ := conn.Do("GET", lock_key)
	return lock != nil
}
func (l redisLocker) AcquireLock(key string, timeout time.Duration) Lock {
	var conn = l.pool.Get()
	defer conn.Close()
	lock_key := key + ".lock"
	combo := strconv.Itoa(rand.Int())
	lock, lock_err := conn.Do("SET", lock_key, combo, "NX", "EX", timeout.Seconds())
	if lock == nil || lock_err != nil {
		return nil
	} else {
		return Lock(redisLock{&l, key, combo})
	}
}
func (l redisLocker) WaitForRelease(key string) (released bool) {
	var conn = l.pool.Get()
	defer conn.Close()
	lock_key := key + ".lock"
	conn.Send("PSUBSCRIBE", lock_key)
	conn.Flush()
	ret, _ := conn.Receive()
	ret, _ = conn.Receive()
	if ret != nil {
		return true
	} else {
		return false
	}
}

type redisLock struct {
	locker *redisLocker
	key    string
	combo  string
}

func (l redisLock) Release() {
	var conn = l.locker.pool.Get()
	defer conn.Close()
	lock_key := l.key + ".lock"
	ret_lock, _ := conn.Do("GET", lock_key)
	ret_lock_str := parseRedisResponse(ret_lock)
	if ret_lock_str == l.combo {
		conn.Send("PUBLISH", lock_key, "done")
		conn.Send("DEL", lock_key)
		conn.Flush()
	}
}

func parseRedisTime(b interface{}) (t time.Time) {
	tstr := string(b.([]byte))
	tsec, err := strconv.ParseInt(tstr, 0, 0)
	if err != nil {
		panic(err)
	}
	return time.Unix(int64(tsec), 0)
}

func parseRedisResponse(d interface{}) string {
	var str string
	switch d.(type) {
	case []interface{}:
		for _, v := range d.([]interface{}) {
			switch v.(type) {
			case []byte:
				str += string(v.([]byte)) + " "
			case int64:
				str += strconv.Itoa(int(v.(int64))) + " "
			case nil:
				str += "<nil> "
			}
		}
	case []byte:
		str = string(d.([]byte))
	}
	return str
}
func (c RedisPCache) formatCacheKey(key string) string {
	return fmt.Sprintf("%+v.%+v", c.Prefix, key)
}
