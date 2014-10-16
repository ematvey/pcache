#!/usr/bin/env sh
GOPATH="`pwd`"

go get github.com/garyburd/redigo/redis
go get github.com/golang/glog

rm -fr pkg
go install pcache_cli
