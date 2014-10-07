// Proxy - universal cache filling algorithm
//
// - if cache contains target item:
// -- return it
// -- if its age is more then obsoletion duration, launch re-{{fetch}}
// - if cache doesn't contain target item or item is null or is invalid (validation checker might be passed):
// -- launch {{fetch}}. if it returns before *timeout* and without error, return item, otherwise nil
//
// fetch:
// - check fetching lock, if exists, wait until it is released or *timeout*, take item from cache and return it
// - otherwise, check last_fetch timestamp, if it is > *throttle*, return nil
// - otherwise, call fetcher into separate goroutine and update last_fetch timestamp;
//   if fetcher completes before timeout, return item, otherwise return nil;
//   when fetcher completes, put item into cache deferred action;
package pcache

import (
	"errors"
	"reflect"
	"time"
)

func proxy(
	store redisCacheBackend,
	locker redisLocker,

	key string,
	target interface{},
	fetcher func() (interface{}, error),

	expire time.Duration,
	refresh time.Duration,
	throttle time.Duration,
	timeout time.Duration,
) error {
	var getAfterRelease = func() bool {
		done_ch := make(chan bool, 1)
		go func() {
			done_ch <- locker.WaitForRelease(key)
		}()
		select {
		case <-time.After(timeout):
			return false
		case ok := <-done_ch:
			if ok {
				retrieved, _, _ := store.Get(key, target)
				return retrieved // TODO: optimize
			}
			return false
		}
	}
	var fetchAndSet = func() chan interface{} {
		ch := make(chan interface{}, 1)
		go func() {
			lock := locker.AcquireLock(key, timeout)
			if lock == nil {
				ch <- getAfterRelease()
				return
			}
			item, _ := fetcher()
			ch <- item
			store.Set(key, item, expire)
			lock.Release()
		}()
		return ch
	}

	retrieved, creationTime, lastFetchTime := store.Get(key, target)

	switch {

	case retrieved:
		if creationTime != nil {
			now := time.Now()
			age := now.Sub(*creationTime)
			sinceLastFetch := now.Sub(*lastFetchTime)
			if age > refresh {
				if sinceLastFetch > throttle {
					go func() {
						<-fetchAndSet()
					}()
				}
			}
		}
		return nil

	case locker.IsLocked(key):
		if getAfterRelease() {
			return nil
		} else {
			return errors.New("getAfterRelease not ok")
		}

	default:
		if lastFetchTime != nil {
			sinceLastFetch := time.Now().Sub(*lastFetchTime)
			if sinceLastFetch < throttle {
				return errors.New("refresh throttled while item is nil")
			}
		}
		select {
		case result := <-fetchAndSet():
			return setToValue(target, result)
		case <-time.After(timeout):
			return errors.New("timeout")
		}

	}
}

func setToValue(target, value interface{}) error {
	var tgt, val reflect.Value
	tgt = reflect.Indirect(reflect.ValueOf(target))
	val = reflect.ValueOf(value)
	if val.Type() == tgt.Type() {
		tgt.Set(val)
	} else {
		val = reflect.Indirect(val)
		if val.Type() == tgt.Type() {
			tgt.Set(val)
		}
	}
	return nil
}
