package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go_kafka_client"
	"log"
	"os"
	"testing"
	"time"
)

var brokers = []string{"go-broker:9092"}
var zookeepers = []string{"go-zookeeper:2181"}

var timeout = 30 * time.Second

func sendAndConsumeRoutine(t *testing.T, quit chan int) {
	testTopic := fmt.Sprintf("test-simple-%d", time.Now().Unix())
	testGroupId := fmt.Sprintf("group-%d", time.Now().Unix())
	testMessage := fmt.Sprintf("test-message-%d", time.Now().Unix())

	fmt.Println("Starting sample broker testing")
	go_kafka_client.CreateMultiplePartitionsTopic(zookeepers[0], testTopic, 1)
	go_kafka_client.EnsureHasLeader(zookeepers[0], testTopic)
	kafkaProducer := producer.NewKafkaProducer(testTopic, brokers)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic)
	err := kafkaProducer.SendStringSync(testMessage)
	if err != nil {
		quit <- 1
		t.Fatalf("Failed to produce message: %v", err)
	}
	kafkaProducer.Close()

	messageConsumed := false
	kafkaConsumer := createConsumer(testGroupId, func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		message := string(msg.Value)
		if message != testMessage {
			t.Errorf("Produced value %s and consumed value %s do not match.", testMessage, message)
		} else {
			fmt.Printf("Consumer %d successfully consumed a message %s\n", 1, message)
		}
		messageConsumed = true
		quit <- 1

		return go_kafka_client.NewSuccessfulResult(taskId)
	})

	fmt.Printf("Trying to consume the message with group %s\n", testGroupId)
	go kafkaConsumer.StartStatic(map[string]int{
		testTopic: 1,
	})

	time.Sleep(timeout)
	if !messageConsumed {
		t.Errorf("Failed to produce and consume a value within %s", timeout)
	}
	<-kafkaConsumer.Close()
	quit <- 1
}

func sendAndConsumeGroupsRoutine(t *testing.T, quit chan int) {
	testTopic := fmt.Sprintf("test-groups-%d", time.Now().Unix())
	testGroupId := fmt.Sprintf("test-group1-%d", time.Now().Unix())
	testGroupId2 := fmt.Sprintf("test-group2-%d", time.Now().Unix())
	testMessage := fmt.Sprintf("test-message-%d", time.Now().Unix())

	fmt.Println("Starting sample broker testing")
	go_kafka_client.CreateMultiplePartitionsTopic(zookeepers[0], testTopic, 1)
	go_kafka_client.EnsureHasLeader(zookeepers[0], testTopic)
	kafkaProducer := producer.NewKafkaProducer(testTopic, brokers)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic)
	err := kafkaProducer.SendStringSync(testMessage)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
		quit <- 1
		quit <- 1
	}

	messageCount := 0
	readFunc := func(consumerId int) go_kafka_client.WorkerStrategy {
		return func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
			message := string(msg.Value)
			if message != testMessage {
				t.Errorf("Produced value %s and consumed value %s do not match.", testMessage, message)
			} else {
				fmt.Printf("Consumer %d successfully consumed a message %s\n", consumerId, message)
			}
			messageCount++
			quit <- 1

			return go_kafka_client.NewSuccessfulResult(taskId)
		}
	}

	consumer1 := createConsumer(testGroupId, readFunc(1))
	fmt.Printf("Trying to consume the message with Consumer 1 and group %s\n", testGroupId)
	go consumer1.StartStatic(map[string]int{
		testTopic: 1,
	})

	consumer2 := createConsumer(testGroupId2, readFunc(2))
	fmt.Printf("Trying to consume the message with Consumer 2 and group %s\n", testGroupId2)
	go consumer2.StartStatic(map[string]int{
		testTopic: 1,
	})
	time.Sleep(timeout)
	if messageCount != 2 {
		t.Errorf("Failed to produce and consume a value within %s", timeout)
	}

	<-consumer1.Close()
	<-consumer2.Close()

	quit <- 1
	quit <- 1
}

func consumerGroupsSinglePartitionRoutine(t *testing.T, quit chan int) {
	topic := fmt.Sprintf("single-partition-%d", time.Now().Unix())
	consumerGroup1 := fmt.Sprintf("single-partition-%d", time.Now().Unix())
	go_kafka_client.CreateMultiplePartitionsTopic(zookeepers[0], topic, 1)
	go_kafka_client.EnsureHasLeader(zookeepers[0], topic)

	//create a new producer and send 2 messages to a random topic
	kafkaProducer := producer.NewKafkaProducer(topic, brokers)
	fmt.Printf("Sending message 1 and 2 to topic %s\n", topic)
	kafkaProducer.SendStringSync("1")
	kafkaProducer.SendStringSync("2")

	//create a new consumer and try to consume the 2 produced messages
	waiter1 := make(chan int)
	messageCount1 := 0
	consumer1 := createConsumer(consumerGroup1, func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		fmt.Printf("Consumed message %s\n", string(msg.Value))
		messageCount1++
		if messageCount1 == 2 {
			waiter1 <- 1
		}

		return go_kafka_client.NewSuccessfulResult(taskId)
	})
	fmt.Printf("Trying to consume messages with Consumer 1 and group %s\n", consumerGroup1)
	go consumer1.StartStatic(map[string]int{
		topic: 1,
	})

	//wait until the messages are consumed or time out after 10 seconds
	select {
	case <-waiter1:
		//wait a bit to commit offset
		time.Sleep(2 * time.Second)
	case <-time.After(timeout):
		t.Errorf("Failed to consume messages with Consumer 1 within %s", timeout)
	}
	consumer1.Close()

	//create one more consumer with the same consumer group and make sure messages are not consumed again
	waiter2 := make(chan int)
	consumer2 := createConsumer(consumerGroup1, func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		t.Errorf("Consumer 2 consumed a previously consumed message %s\n", string(msg.Value))
		waiter2 <- 1
		return go_kafka_client.NewSuccessfulResult(taskId)
	})
	fmt.Printf("Trying to consume messages with Consumer 2 and group %s\n", consumerGroup1)
	go consumer2.StartStatic(map[string]int{
		topic: 1,
	})

	fmt.Println("wait to make sure messages are not consumed again")
	select {
	case <-waiter2:
	case <-time.After(5 * time.Second):
	}
	consumer2.Close()

	fmt.Println("produce 50 more messages")
	numMessages := 50
	for i := 0; i < numMessages; i++ {
		kafkaProducer.SendStringSync(fmt.Sprintf("message-%d", i))
	}

	fmt.Println("consume these messages with a consumer group")
	//total number of consumed messages should be 50 i.e. no duplicate or missing messages within one group
	messageCount2 := 0
	consumer3 := createConsumer(consumerGroup1, func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		fmt.Printf("Consumer 1 consumed message %s\n", string(msg.Value))
		messageCount2++
		return go_kafka_client.NewSuccessfulResult(taskId)
	})
	go consumer3.StartStatic(map[string]int{
		topic: 1,
	})

	<-time.After(timeout / 2)
	if messageCount2 != numMessages {
		t.Errorf("Invalid number of messages: expected %d, actual %d", numMessages, messageCount2)
	} else {
		fmt.Printf("Consumed %d messages\n", messageCount2)
	}

	//shutdown gracefully
	kafkaProducer.Close()
	<-consumer1.Close()
	<-consumer2.Close()
	<-consumer3.Close()

	quit <- 1
}

func consumerGroupsMultiplePartitionsRoutine(t *testing.T, quit chan int) {
	topic := fmt.Sprintf("multiple-partition-%d", time.Now().Unix())
	go_kafka_client.CreateMultiplePartitionsTopic(zookeepers[0], topic, 2)
	go_kafka_client.EnsureHasLeader(zookeepers[0], topic)

	kafkaProducer := producer.NewKafkaProducer(topic, brokers)
	totalMessages := 100
	fmt.Printf("Sending %d messages to topic %s\n", totalMessages, topic)
	go func() {
		for i := 0; i < totalMessages; i++ {
			kafkaProducer.SendStringSync(fmt.Sprintf("partitioned %d", i))
		}
	}()

	fmt.Println("consume these messages with a consumer group")
	messageCount := 0
	consumer1 := createConsumer("group1", func(worker *go_kafka_client.Worker, msg *go_kafka_client.Message, taskId go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		fmt.Printf("Consumer group consumed message %s\n", string(msg.Value))
		messageCount++
		return go_kafka_client.NewSuccessfulResult(taskId)
	})
	go consumer1.StartStatic(map[string]int{
		topic: 1,
	})

	<-time.After(timeout / 2)
	if messageCount != totalMessages {
		t.Errorf("Invalid number of messages: expected %d, actual %d", totalMessages, messageCount)
	} else {
		fmt.Printf("Consumed %d messages\n", messageCount)
	}

	//shutdown gracefully
	kafkaProducer.Close()
	consumer1.Close()
	quit <- 1
}

func TestSendAndConsume(t *testing.T) {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	quit := make(chan int)
	go sendAndConsumeRoutine(t, quit)

	<-quit
}

func TestSendAndConsumeGroups(t *testing.T) {
	quit := make(chan int)
	go sendAndConsumeGroupsRoutine(t, quit)

	<-quit
	<-quit
}

func TestConsumerGroupsSinglePartition(t *testing.T) {
	quit := make(chan int)

	go consumerGroupsSinglePartitionRoutine(t, quit)
	<-quit
}

func TestConsumerGroupsMultiplePartitions(t *testing.T) {
	quit := make(chan int)

	go consumerGroupsMultiplePartitionsRoutine(t, quit)
	<-quit
}

func createConsumer(group string, workerStrategy go_kafka_client.WorkerStrategy) *go_kafka_client.Consumer {
	//Coordinator settings
	zookeeperConfig := go_kafka_client.NewZookeeperConfig()
	zookeeperConfig.ZookeeperConnect = []string{zookeeper}

	//Actual consumer settings
	consumerConfig := go_kafka_client.DefaultConsumerConfig()
	consumerConfig.Coordinator = go_kafka_client.NewZookeeperCoordinator(zookeeperConfig)
	consumerConfig.Groupid = group
	consumerConfig.NumWorkers = 1
	consumerConfig.NumConsumerFetchers = 1
	consumerConfig.FetchBatchSize = 1
	consumerConfig.FetchBatchTimeout = 1 * time.Second
	consumerConfig.Strategy = workerStrategy
	consumerConfig.AutoOffsetReset = go_kafka_client.SmallestOffset
	consumerConfig.WorkerFailureCallback = func(*go_kafka_client.WorkerManager) go_kafka_client.FailedDecision {
		return go_kafka_client.CommitOffsetAndContinue
	}
	consumerConfig.WorkerFailedAttemptCallback = func(*go_kafka_client.Task, go_kafka_client.WorkerResult) go_kafka_client.FailedDecision {
		return go_kafka_client.CommitOffsetAndContinue
	}

	return go_kafka_client.NewConsumer(consumerConfig)
}
