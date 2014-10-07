package pcache

import (
	"time"
)

type Cache interface {
	Set(string, interface{}, time.Duration) error
	Get(string, interface{}) error
	Ttl(string) (time.Duration, error)
}
