package main

import (
	"fmt"
	"pcache"
	"time"
)

func main() {
	ch := pcache.RedisPCacheNew("test", "tcp", "localhost:6379")
	var tgt string
	err := ch.Call(
		"key",
		&tgt,
		func() (interface{}, error) { time.Sleep(time.Second * 2); return "abc", nil },
		// func(result interface{}) bool {
		// 	str, ok := result.(string)
		// 	if !ok {
		// 		return false
		// 	}
		// 	if str == "" {
		// 		return false
		// 	}
		// 	return true
		// },
		nil,
		time.Second*10,
		time.Second*10,
		time.Second*1,
		time.Second*3,
	)

	fmt.Printf("item: %+v, err: %+v\n", tgt, err)
}
