#!/bin/sh

export PATH=$PATH:/usr/local/go/bin
export GOPATH=/usr/local/go/home/
export KAFKA_PATH=/opt/apache/kafka

go test -v
