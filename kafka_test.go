package main

import (
	"testing"
	"fmt"
	"time"
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go-kafka/consumer"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"github.com/stealthly/go_kafka_client"
	"github.com/wvanbergen/kafka/consumergroup"
)

var brokers = []string{"go-broker:9092"}
var zookeepers = []string{"go-zookeeper:2181"}

var timeout = 20 * time.Second

func sendAndConsumeRoutine(t *testing.T, quit chan int) {
	testTopic := fmt.Sprintf("test-simple-%d", time.Now().Unix())
	testGroupId := fmt.Sprintf("group-%d", time.Now().Unix())
	testMessage := fmt.Sprintf("test-message-%d", time.Now().Unix())

	fmt.Println("Starting sample broker testing")
	go_kafka_client.CreateMultiplePartitionsTopic(zookeepers[0], testTopic, 1)
	go_kafka_client.EnsureHasLeader(zookeepers[0], testTopic)
	kafkaProducer := producer.NewKafkaProducer(testTopic, brokers)
	defer kafkaProducer.Close()
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic)
	err := kafkaProducer.SendStringSync(testMessage)
	if (err != nil) {
		quit <- 1
		t.Fatalf("Failed to produce message: %v", err)
	}

	kafkaConsumer := consumer.NewKafkaConsumer(testTopic, testGroupId, brokers, nil)
	defer kafkaConsumer.Close()
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

	time.Sleep(timeout)
	if !messageConsumed {
		t.Errorf("Failed to produce and consume a value within %s", timeout)
	}
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
	if (err != nil) {
		t.Fatalf("Failed to produce message: %v", err)
		quit <- 1
		quit <- 1
	}

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

	consumer1 := consumer.NewKafkaConsumer(testTopic, testGroupId, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 1 and group %s\n", testGroupId)
	go consumer1.Read(readFunc(1))

	consumer2 := consumer.NewKafkaConsumer(testTopic, testGroupId2, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 2 and group %s\n", testGroupId2)
	go consumer2.Read(readFunc(2))
	time.Sleep(timeout)
	if (messageCount != 2) {
		t.Errorf("Failed to produce and consume a value within %s", timeout)
	}
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
	config := consumergroup.NewConsumerGroupConfig()
	config.CommitInterval = 1 * time.Second
	consumer1 := consumer.NewKafkaConsumerGroup(topic, consumerGroup1, zookeepers, config)
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
	case <-time.After(timeout):
		t.Errorf("Failed to consume messages with Consumer 1 within %s", timeout)
	}
	consumer1.Close()

	//create one more consumer with the same consumer group and make sure messages are not consumed again
	config = consumergroup.NewConsumerGroupConfig()
	config.CommitInterval = 1 * time.Second
	consumer2 := consumer.NewKafkaConsumerGroup(topic, consumerGroup1, zookeepers, config)
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
	consumer2.Close()

	fmt.Println("produce 50 more messages")
	numMessages := 50
	for i := 0; i < numMessages; i++ {
		kafkaProducer.SendStringSync(fmt.Sprintf("message-%d", i))
	}

	fmt.Println("consume these messages with a consumer group")
	config = consumergroup.NewConsumerGroupConfig()
	config.CommitInterval = 1 * time.Second
	consumer3 := consumer.NewKafkaConsumerGroup(topic, consumerGroup1, zookeepers, config)
	//total number of consumed messages should be 50 i.e. no duplicate or missing messages within one group
	messageCount2 := 0
	go consumer3.Read(func(bytes []byte) {
		fmt.Printf("Consumer 1 consumed message %s\n", string(bytes))
		messageCount2++
	})

	<-time.After(timeout / 2)
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
	config := consumergroup.NewConsumerGroupConfig()
	config.CommitInterval = 1 * time.Second
	consumer1 := consumer.NewKafkaConsumerGroup(topic, "group1", zookeepers, config)

	messageCount := 0

	go consumer1.Read(func(bytes []byte) {
		fmt.Printf("Consumer group consumed message %s\n", string(bytes))
		messageCount++
	})

	<-time.After(timeout / 2)
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
