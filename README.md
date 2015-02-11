go-kafka running on Mesos
========================

This repository provides the Mesos Scheduler and Executor implementations to run [https://github.com/stealthly/go_kafka_client](https://github.com/stealthly/go_kafka_client) on Mesos.

Pre-Requisites
==============

- [Golang](http://golang.org/doc/install)   
- A standard and working Go workspace setup   
- [godep](https://github.com/tools/godep)   
- Apache Mesos 0.19 or newer

Build Instructions
=================

- Get the project   
```
$ cd $GOPATH/src/
$ mkdir -p github.com/stealthly
$ cd github.com/stealthly
$ git clone https://github.com/stealthly/go-kafka.git
$ cd go-kafka
$ godep restore
```

- Modify `$GOPATH/src/github.com/stealthly/go-kafka/mesos/consumer_config.go` to provide your consumer configurations.
- Build the scheduler and the executor
```
$ cd $GOPATH/src/github.com/stealthly/go-kafka
$ go build -tags scheduler framework.go
$ go build -tags executor executor.go
```
- Package the executor (**make sure the built binary has executable permissions before this step!**)
```
$ zip -r executor.zip executor
```
- Place the built framework and executor archive somewhere on Mesos Master node

Running
=======

You will need a running Mesos master and slaves to run. The following commands should be launched on Mesos Master node.

**Static partitioning configuration:**   
```
$ cd <framework-location>
$ ./framework --master master:5050 --zookeeper master:2181 --group mesos-group-1 --whitelist="^mesos-topic$"
```

**Load balancing configuration:**
```
$ cd <framework-location>
$ ./framework --master master:5050 --zookeeper master:2181 --group mesos-group-1 --whitelist="^mesos-topic$" --static=false --num.consumers 2
```

*List of available flags:*

```
--artifact.host="master": Binding host for artifact server.
--artifact.port=6666: Binding port for artifact server.
--blacklist="": Blacklist of topics to consume.
--cpu.per.consumer=1: CPUs per consumer instance.
--executor.archive="executor.zip": Executor archive name. Absolute or relative path are both ok.
--executor.name="executor": Executor binary name contained in archive.
--group="": Consumer group name to start consumers in.
--log.level="info": Log level for built-in logger.
--master="127.0.0.1:5050": Mesos Master address <ip:port>.
--mem.per.consumer=256: Memory per consumer instance.
--num.consumers=1: Number of consumers for non-static configuration.
--static=true: Flag to use static partition configuration. Defaults to true. <num.consumers> is ignored when set to true.
--whitelist="": Whitelist of topics to consume.
--zookeeper="": Zookeeper connection string separated by comma.
```

Other Mesos unrelated stuff
==========================

Quick up and running using Go for Apache Kafka

Use Vagrant to get up and running.

1) Install Vagrant [http://www.vagrantup.com/](http://www.vagrantup.com/)  
2) Install Virtual Box [https://www.virtualbox.org/](https://www.virtualbox.org/)  

In the main kafka folder  

1) vagrant up   
2) vagrant ssh brokerOne   
3) cd /vagrant   
4) sudo ./test.sh

once this is done 
* Zookeeper will be running 192.168.86.5
* Broker 1 on 192.168.86.10
* All the tests in github.com/stealthly/go-kafka/test/* should pass  

You can access the brokers and zookeeper by their IP from your local without having to go into vm.

e.g.

bin/kafka-console-producer.sh --broker-list 192.168.86.10:9092 --topic *get this from the random topic created in test*

bin/kafka-console-consumer.sh --zookeeper 192.168.86.5:2181 --topic *get this from the random topic created in test* --from-beginning

If there is an API break, `godep` provides facilities for building and running the provided dependency snapshot (`godep save`). To leverage this, simply prepend all go commands with `godep` to run them inside a sandbox. Example:
```bash
# Run Go ping-pong client
godep go run scala_go_kafka.go go-topic scala-topic
```