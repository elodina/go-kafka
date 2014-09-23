package main

import (
	"testing"
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"time"
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go-kafka/consumer"
)

var brokers = []string{"go-broker:9092"}
var zookeeper = []string{"go-zookeeper:2181"}
var singlePartitionsTopic = "single_partition"
var multiplePartitionsTopic = "partitions_test"

var timeout time.Duration = 10

var testMessage = uuid.New()
var testTopic = uuid.New()
var testTopic2 = uuid.New()
var testGroupId = uuid.New()
var testGroupId2 = uuid.New()

func sendAndConsumeRoutine(t *testing.T, quit chan int) {
	fmt.Println("Starting sample broker testing")
	kafkaProducer := producer.NewKafkaProducer(testTopic, brokers, nil)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic)
	kafkaProducer.Send(testMessage)

	kafkaConsumer := consumer.NewKafkaConsumer(testTopic, testGroupId, brokers, nil)
	fmt.Printf("Trying to consume the message with group %s\n", testGroupId)
	messageConsumed := false
	go kafkaConsumer.Read(func(bytes []byte) {
		message := string(bytes)
		if (message != testMessage) {
			t.Errorf("Produced value %s and consumed value %s do not match.", testMessage, message)
		} else {
			fmt.Printf("Consumer %d successfully consumed a message %s\n", 1, message)
		}
		messageConsumed = true
		quit <- 1
	})
	time.Sleep(timeout * time.Second)
	if !messageConsumed {
		t.Errorf("Failed to produce and consume a value within %d seconds", timeout)
	}
	quit <- 1
}

func sendAndConsumeGroupsRoutine(t *testing.T, quit chan int) {
	fmt.Println("Starting sample broker testing")
	kafkaProducer := producer.NewKafkaProducer(testTopic2, brokers, nil)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic2)
	kafkaProducer.Send(testMessage)

	messageCount := 0
	readFunc := func(consumerId int) func([]byte) {
		return func(bytes []byte) {
			message := string(bytes)
			if (message != testMessage) {
				t.Errorf("Produced value %s and consumed value %s do not match.", testMessage, message)
			} else {
				fmt.Printf("Consumer %d successfully consumed a message %s\n", consumerId, message)
			}
			messageCount++
			quit <- 1
		}
	}

	consumer1 := consumer.NewKafkaConsumer(testTopic2, testGroupId, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 1 and group %s\n", testGroupId)
	go consumer1.Read(readFunc(1))

	consumer2 := consumer.NewKafkaConsumer(testTopic2, testGroupId2, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 2 and group %s\n", testGroupId2)
	go consumer2.Read(readFunc(2))
	time.Sleep(timeout * time.Second)
	if (messageCount != 2) {
		t.Errorf("Failed to produce and consume a value within %d seconds", timeout)
	}
	quit <- 1
	quit <- 1
}

func consumerGroupsSinglePartitionRoutine(t *testing.T, quit chan int) {
	consumerGroup1 := uuid.New()

	//create a new producer and send 2 messages to a random topic
	kafkaProducer := producer.NewKafkaProducer(singlePartitionsTopic, brokers, nil)
	fmt.Printf("Sending message 1 and 2 to topic %s\n", singlePartitionsTopic)
	kafkaProducer.Send("1")
	kafkaProducer.Send("2")

	//create a new consumer and try to consume the 2 produced messages
	consumer1 := consumer.NewKafkaConsumerGroup(singlePartitionsTopic, consumerGroup1, zookeeper, nil)
	fmt.Printf("Trying to consume messages with Consumer 1 and group %s\n", consumerGroup1)
	messageCount1 := 0
	waiter1 := make(chan int)
	go consumer1.Read(func(bytes []byte) {
		fmt.Printf("Consumed message %s\n", string(bytes))
		messageCount1++
		if (messageCount1 == 2) {
			waiter1 <- 1
		}
	})

	//wait until the messages are consumed or time out after 10 seconds
	select {
	case <-waiter1:
		//wait a bit to commit offset
		time.Sleep(2 * time.Second)
	case <-time.After(timeout * time.Second):
		t.Errorf("Failed to consume messages with Consumer 1 within %d seconds", timeout)
	}

	//create one more consumer with the same consumer group and make sure messages are not consumed again
	consumer2 := consumer.NewKafkaConsumerGroup(singlePartitionsTopic, consumerGroup1, zookeeper, nil)
	fmt.Printf("Trying to consume messages with Consumer 2 and group %s\n", consumerGroup1)
	waiter2 := make(chan int)
	go consumer2.Read(func(bytes []byte) {
		t.Errorf("Consumer 2 consumed a previously consumed message %s\n", string(bytes))
		waiter2 <- 1
	})

	fmt.Println("wait to make sure messages are not consumed again")
	select {
	case <-waiter2:
	case <-time.After(5 * time.Second):
	}

	//shutdown consumers so that they don't consume future 50 messages instantly
	consumer1.Close()
	consumer2.Close()

	fmt.Println("produce 50 more messages")
	numMessages := 50
	for i := 3; i < 3+numMessages; i++ {
		kafkaProducer.Send(uuid.New())
	}

	fmt.Println("consume these messages with a consumer group")
	consumer3 := consumer.NewKafkaConsumerGroup(singlePartitionsTopic, consumerGroup1, zookeeper, nil)
	//total number of consumed messages should be 50 i.e. no duplicate or missing messages within one group
	messageCount2 := 0
	go consumer3.Read(func(bytes []byte) {
		fmt.Printf("Consumer 1 consumed message %s\n", string(bytes))
		messageCount2++
	})

	<-time.After(timeout / 2 * time.Second)
	if (messageCount2 != numMessages) {
		t.Errorf("Invalid number of messages: expected %d, actual %d", numMessages, messageCount2)
	} else {
		fmt.Printf("Consumed %d messages\n", messageCount2)
	}

	//shutdown gracefully
	kafkaProducer.Close()
	consumer3.Close()
	quit <- 1
}

func consumerGroupsMultiplePartitionsRoutine(t *testing.T, quit chan int) {
	kafkaProducer := producer.NewKafkaProducer(multiplePartitionsTopic, brokers, nil)
	totalMessages := 100
	fmt.Printf("Sending %d messages to topic %s\n", totalMessages, multiplePartitionsTopic)
	go func() {
		for i := 0; i < totalMessages; i++ {
			kafkaProducer.Send(fmt.Sprintf("partitioned %d", i))
		}
	}()

	fmt.Println("consume these messages with a consumer group")
	consumer1 := consumer.NewKafkaConsumerGroup(multiplePartitionsTopic, "group1", zookeeper, nil)

	messageCount := 0

	go consumer1.Read(func(bytes []byte) {
		fmt.Printf("Consumer group consumed message %s\n", string(bytes))
		messageCount++
	})

	<-time.After(timeout / 2 * time.Second)
	if (messageCount != totalMessages) {
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
