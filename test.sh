#!/bin/sh

export PATH=$PATH:/usr/local/go/bin
export GOPATH=/vagrant/vagrant/go/

go test -v github.com/stealthly/go-kafka/test