package pcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type redisCacheBackend struct {
	pool   *redis.Pool
	prefix string
}

func (r redisCacheBackend) Get(
	key string, target interface{},
) (
	retrieved bool, creation_time *time.Time, last_fetch_time *time.Time,
) {
	os.Stdout.Write([]byte("redisCacheBackend.Get started\n"))
	defer func() {
		os.Stdout.Write([]byte(fmt.Sprintf("redisCacheBackend.Get done (retrieved: %+v, created: %+v, fetch: %+v)\n", retrieved, creation_time, last_fetch_time)))
	}()
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
func (r redisCacheBackend) Set(key string, item interface{}, expire time.Duration) error {
	os.Stdout.Write([]byte("redisCacheBackend.Set started\n"))
	defer func() {
		os.Stdout.Write([]byte("redisCacheBackend.Set done\n"))
	}()
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
	os.Stdout.Write([]byte("redisLocker.IsLocked started\n"))
	defer func() {
		os.Stdout.Write([]byte("redisLocker.IsLocked done\n"))
	}()
	var conn = l.pool.Get()
	defer conn.Close()
	lock_key := key + ".lock"
	lock, _ := conn.Do("GET", lock_key)
	return lock != nil
}
func (l redisLocker) AcquireLock(key string, timeout time.Duration) *redisLock {
	var conn = l.pool.Get()
	defer conn.Close()
	lock_key := key + ".lock"
	combo := strconv.Itoa(rand.Int())
	lock, lock_err := conn.Do("SET", lock_key, combo, "NX", "EX", timeout.Seconds())
	if lock == nil || lock_err != nil {
		return nil
	} else {
		return &redisLock{&l, key, combo}
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
