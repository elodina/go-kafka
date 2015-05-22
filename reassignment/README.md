Partition Reassignment Tool
===========================

This tool provides a way to reassign specified amount of partitions of a Kafka topic. Basically it's just a wrapper over `kafka-reassign-partitions.sh` but provides a bit more flexible way to do the same things.

Usage
====

This tool does not depend on anything but Golang. This said you should have a standard and working Go workspace setup.

This tool requires a path to local Kafka distribution either via `--kafka.path` flag or `$KAFKA_PATH` env variable.

Supports 3 modes, exactly one of following is required:

- `--generate` - this also requires `--topic`, `--partitions` and `--broker.list` flags set. `--generate` will analyze current cluster layout and make a candidate reassignment for specified topic and amount of partitions if clusted is not balanced. You will be proposed to apply given configuration or save it to a file after this step if the cluster is not balanced.
- `--execute` - this also requires `--reassignment` flag set. `--execute` will execute the reassignment candidate located in specified flag. You will be proposed to verify the given configuration after this step.
- `--verify` - this also requires `--reassignment` flag set. `--verify` will verify the reassignment candidate located in specified flag.

Usage Examples
=============

```
$ export KAFKA_PATH=/path/to/kafka/
```

**generate**

```
$ go run reassignment.go --zookeeper localhost:2181 --topic logs --partitions 1 --broker.list="1,2,3" --generate
```

**execute**

```
$ go run reassignment.go --zookeeper localhost:2181 --reassignment new_topics.json --execute
```

**verify**

```
$ go run reassignment.go --zookeeper localhost:2181 --reassignment new_topics.json --verify
```