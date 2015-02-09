// +build executor

package main

import (
	kafka "github.com/stealthly/go_kafka_client"
	"github.com/mesos/mesos-go/executor"
	"fmt"
	"github.com/stealthly/go-kafka/mesos"
	"flag"
	"os"
	"strings"
)

var zookeeper = flag.String("zookeeper", "", "Zookeeper connection string separated by comma.")
var group = flag.String("group", "", "Consumer group name to start consumers in.")
var topic = flag.String("topic", "", "Topic to consume.")
var partition = flag.Int("partition", 0, "Partition to consume. Defaults to 0.")
var whitelist = flag.String("whitelist", "", "Whitelist of topics to consume.")
var blacklist = flag.String("blacklist", "", "Blacklist of topics to consume.")

func parseAndValidateExecutorArgs() {
	flag.Parse()

	if *zookeeper == "" {
		fmt.Println("Zookeeper connection string is required.")
		os.Exit(1)
	}

	isStatic := *whitelist == "" && *blacklist == ""

	if isStatic {
		if *topic == "" {
			fmt.Println("Topic to consume is required.")
			os.Exit(1)
		}

		if *partition < 0 {
			fmt.Println("Partition to consume should be >= 0.")
			os.Exit(1)
		}
	}

	if *group == "" {
		fmt.Println("Consumer group name is required.")
		os.Exit(1)
	}
}

func main() {
	parseAndValidateExecutorArgs()
	fmt.Println("Starting Go Kafka Client Executor")

	executorConfig := mesos.NewExecutorConfig()

	if *whitelist != "" {
		executorConfig.Filter = kafka.NewWhiteList(*whitelist)
	} else if *blacklist != "" {
		executorConfig.Filter = kafka.NewBlackList(*blacklist)
	}

	executorConfig.Zookeeper = strings.Split(*zookeeper, ",")
	executorConfig.Group = *group
	executorConfig.Topic = *topic
	executorConfig.Partition = int32(*partition)
	driver, err := executor.NewMesosExecutorDriver(mesos.NewGoKafkaClientExecutor(executorConfig))

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}
