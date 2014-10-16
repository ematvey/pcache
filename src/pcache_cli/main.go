package main

import (
	"fmt"
	"pcache"
	"time"
)

func main() {
	ch := pcache.RedisPCacheNew("test", "tcp", "localhost:6379")
	var tgt string
	err := ch.Proxy(
		"key",
		&tgt,
		func() (interface{}, error) { time.Sleep(time.Second * 3); return "abc", nil },
		func(result interface{}) bool {
			str, ok := result.(string)
			if !ok {
				return false
			}
			if str == "" {
				return false
			}
			return true
		},
		time.Second*10,
		time.Second*3,
		time.Second*1,
		time.Second*6,
	)

	ch.Get("key", &tgt)
	ttl, _ := ch.Ttl("key")
	fmt.Printf("item: %+v, ttl: %+v\n", tgt, ttl)
	fmt.Printf("err: %+v\n", err)
}
