/*
ProxyCache - universal cache filling algorithm

Idea

Instead of trying to making cache lookups and determining if you should perform some expensive
calculation, you pass calculation fetcher as a delegate into ProxyCache which will figure out
caching state by itself and use fetcher if necessary.

Parameters

Algorithm considers various parameters that might affect caching behavior: item expiration,
ttl (specified per-request), request timeout, throttle time between fetcher calls, results
of validation (preformed by validator func).
*/
package pcache

import (
	"errors"
	"reflect"
	"time"
)

type Fetcher func() (interface{}, error)

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

type ResourceSpec struct {
	Store                          Store
	Locker                         Locker
	Validator                      func(interface{}) bool
	Expire, Ttl, Throttle, Timeout time.Duration
}

func ProxyCache(key string, target interface{}, fetcher Fetcher, spec *ResourceSpec) error {
	var expire time.Duration
	if spec.Expire > 0 {
		expire = spec.Expire
	} else {
		expire = -1
	}

	var getAfterRelease = func() bool {
		done_ch := make(chan bool, 1)
		go func() {
			done_ch <- spec.Locker.WaitForRelease(key)
		}()
		select {
		case <-time.After(spec.Timeout):
			return false
		case ok := <-done_ch:
			if ok {
				retrieved, _, _ := spec.Store.Get(key, target)
				return retrieved // TODO: optimize
			}
			return false
		}
	}
	var fetchAndSet = func() chan interface{} {
		ch := make(chan interface{}, 1)
		go func() {
			lock := spec.Locker.AcquireLock(key, spec.Timeout)
			if lock == nil {
				ch <- getAfterRelease()
				return
			}
			defer lock.Release()
			item, _ := fetcher()
			if spec.Validator(item) == true {
				ch <- item
				spec.Store.Set(key, item, expire)
			} else {
				ch <- nil
			}
		}()
		return ch
	}

	retrieved, creationTime, lastFetchTime := spec.Store.Get(key, target)

	switch {

	case retrieved:
		var invalid = false
		if spec.Validator != nil {
			invalid = spec.Validator(target) == false
		}
		if creationTime != nil || invalid {
			var now = time.Now()
			var age = now.Sub(*creationTime)
			var sinceLastFetch = now.Sub(*lastFetchTime)
			if age > spec.Ttl || invalid {
				if sinceLastFetch > spec.Throttle {
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

	case spec.Locker.IsLocked(key):
		if getAfterRelease() {
			return nil
		} else {
			return errors.New("getAfterRelease not ok")
		}

	default:
		if lastFetchTime != nil {
			var sinceLastFetch = time.Now().Sub(*lastFetchTime)
			if sinceLastFetch < spec.Throttle {
				return errors.New("refresh throttled while item is nil")
			}
		}
		select {
		case result := <-fetchAndSet():
			if result == nil {
				return errors.New("invalid item fetched")
			}
			return copyValue(target, result)
		case <-time.After(spec.Timeout):
			return errors.New("timeout")
		}

	}
}

func copyValue(destination, source interface{}) error {
	var tgt, val reflect.Value
	tgt = reflect.Indirect(reflect.ValueOf(destination))
	val = reflect.ValueOf(source)
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
