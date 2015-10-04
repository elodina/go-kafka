package main

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/stealthly/go-avro"
	"github.com/ods94065/go-kafka/producer"
	"github.com/stealthly/go_kafka_client"
	"os"
	"os/signal"
	"time"
)

var readTopic string
var writeTopic string
var group = "ping-pong-go-group"

var broker = "localhost:9092"
var zookeeper = "localhost:2181"
var schemaRepo = "http://localhost:8081"

var kafkaProducer *producer.KafkaProducer = nil
var kafkaConsumer *go_kafka_client.Consumer = nil

var encoder = go_kafka_client.NewKafkaAvroEncoder(schemaRepo)

func main() {
	parseArgs()

	go_kafka_client.Logger = go_kafka_client.NewDefaultLogger(go_kafka_client.ErrorLevel)
	kafkaProducer = producer.NewKafkaProducer(writeTopic, []string{broker})

	//Coordinator settings
	zookeeperConfig := go_kafka_client.NewZookeeperConfig()
	zookeeperConfig.ZookeeperConnect = []string{zookeeper}

	//Actual consumer settings
	consumerConfig := go_kafka_client.DefaultConsumerConfig()
	consumerConfig.AutoOffsetReset = go_kafka_client.SmallestOffset
	consumerConfig.Coordinator = go_kafka_client.NewZookeeperCoordinator(zookeeperConfig)
	consumerConfig.Groupid = group
	consumerConfig.NumWorkers = 1
	consumerConfig.NumConsumerFetchers = 1
    consumerConfig.KeyDecoder = go_kafka_client.NewKafkaAvroDecoder(schemaRepo)
    consumerConfig.ValueDecoder = consumerConfig.KeyDecoder

	consumerConfig.Strategy = func(worker *go_kafka_client.Worker, message *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		time.Sleep(2 * time.Second)
		record, ok := message.DecodedValue.(*avro.GenericRecord)
		if !ok {
			panic("Not a *GenericError, but expected one")
		}

		fmt.Printf("golang > received %s\n", fmt.Sprintf("{\"counter\": %d, \"name\": \"%s\", \"uuid\": \"%s\"}", record.Get("counter"), record.Get("name"), record.Get("uuid")))
		modify(record)
		encoded, err := encoder.Encode(record)
		if err != nil {
			panic(err)
		}

		if err := kafkaProducer.SendBytesSync(encoded); err != nil {
			panic(err)
		}

		return go_kafka_client.NewSuccessfulResult(taskId)
	}

	consumerConfig.WorkerFailureCallback = func(_ *go_kafka_client.WorkerManager) go_kafka_client.FailedDecision {
		return go_kafka_client.CommitOffsetAndContinue
	}
	consumerConfig.WorkerFailedAttemptCallback = func(_ *go_kafka_client.Task, _ go_kafka_client.WorkerResult) go_kafka_client.FailedDecision {
		return go_kafka_client.CommitOffsetAndContinue
	}

	kafkaConsumer = go_kafka_client.NewConsumer(consumerConfig)

	pingPongLoop()
}

func parseArgs() {
	if len(os.Args) < 3 {
		panic("Usage: go run scala_go_kafka.go $READ_TOPIC $WRITE_TOPIC")
	}

	readTopic = os.Args[1]
	writeTopic = os.Args[2]
}

func pingPongLoop() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Println("\ngolang > Closing consumer")
		kafkaConsumer.Close()
	}()

	fmt.Println("golang > Started!")
	kafkaConsumer.StartStatic(map[string]int{
		readTopic: 1,
	})
}

func modify(obj *avro.GenericRecord) {
	obj.Set("counter", obj.Get("counter").(int64)+1)
	obj.Set("uuid", uuid.New())
}
