#!/bin/bash

if [ -e env.sh ]; then
    source env.sh;
else
    GOROOT="/opt/go"
fi

PATH="$PATH:$GOROOT/bin"
GOPATH="`pwd`"

go get github.com/garyburd/redigo/redis
go get github.com/golang/glog

rm -fr pkg
go install pcache_cli
