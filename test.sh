#!/bin/sh

export PATH=$PATH:/usr/local/go/bin
export GOPATH=/usr/local/go/home/

go test -v github.com/stealthly/go-kafka/test