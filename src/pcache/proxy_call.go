/*
ProxyCache - universal cache filling algorithm

Basic idea is as follows: instead of trying to making cache lookups and determining if you
should perform some expensive calculation, you pass calculation fetcher as a delegate into
ProxyCache which will figure out caching state by itself and use fetcher if necessary.

Algorithm considers various parameters that might affect caching behavior:
*/
package pcache

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Storage backend is assumed to implement some kind of cache replacement algorithm
type Store interface {
	Get(string, interface{}) (bool, *time.Time, *time.Time) // (key, target) -> (retrieved, creation_time, last_fetch_time)
	Set(string, interface{}, time.Duration) error           // (key, item, expire) -> error
}

type Locker interface {
	IsLocked(string) bool
	AcquireLock(string, time.Duration) Lock // (key, timeout) -> Lock
	WaitForRelease(string) bool             // key -> was_released
}

type Lock interface {
	Release()
}

func PCacheCall(
	store Store,
	locker Locker,

	key string,
	target interface{},

	fetcher func() (interface{}, error),
	validator func(interface{}) bool,

	expire time.Duration,
	ttl time.Duration,
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
			defer lock.Release()
			item, _ := fetcher()
			if validator(item) == true {
				ch <- item
				store.Set(key, item, expire)
			} else {
				ch <- nil
			}
		}()
		return ch
	}

	retrieved, creationTime, lastFetchTime := store.Get(key, target)

	switch {

	case retrieved:
		fmt.Printf("validator: %+v", validator)
		var invalid = false
		if validator != nil {
			invalid = validator(target) == false
		}
		if creationTime != nil || invalid {
			var now = time.Now()
			var age = now.Sub(*creationTime)
			var sinceLastFetch = now.Sub(*lastFetchTime)
			if age > ttl || invalid {
				if sinceLastFetch > throttle {
					go func() {
						<-fetchAndSet()
					}()
				}
			}
		}
		if invalid {
			return errors.New("invalid item retrieved")
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
			var sinceLastFetch = time.Now().Sub(*lastFetchTime)
			if sinceLastFetch < throttle {
				return errors.New("refresh throttled while item is nil")
			}
		}
		select {
		case result := <-fetchAndSet():
			if result == nil {
				return errors.New("invalid item fetched")
			}
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
