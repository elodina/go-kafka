package test

import (
	"testing"
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"time"
	. "github.com/stealthly/go-kafka/producer"
	. "github.com/stealthly/go-kafka/consumer"
)

var brokers = []string{"192.168.86.10:9092"}
var zookeeper = []string{"192.168.86.5:2181"}
var timeout time.Duration = 10

var testMessage = uuid.New()
var testTopic = uuid.New()
var testTopic2 = uuid.New()
var testGroupId = uuid.New()
var testGroupId2 = uuid.New()

func sendAndConsumeRoutine(t *testing.T, quit chan int) {
	fmt.Println("Starting sample broker testing")
	kafkaProducer := NewKafkaProducer(testTopic, brokers)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic)
	kafkaProducer.Send(testMessage)

	kafkaConsumer := NewKafkaConsumer(testTopic, testGroupId, brokers, nil)
	fmt.Printf("Trying to consume the message with group %s\n", testGroupId)
	go kafkaConsumer.Read(readFunc(1, t, quit))
	time.Sleep(timeout * time.Second)
	t.Errorf("Failed to produce and consume a value within %d seconds", timeout)
	quit <- 1
}

func sendAndConsumeGroupsRoutine(t *testing.T, quit chan int) {
	fmt.Println("Starting sample broker testing")
	kafkaProducer := NewKafkaProducer(testTopic2, brokers)
	fmt.Printf("Sending message %s to topic %s\n", testMessage, testTopic2)
	kafkaProducer.Send(testMessage)

	consumer1 := NewKafkaConsumer(testTopic2, testGroupId, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 1 and group %s\n", testGroupId)
	go consumer1.Read(readFunc(1, t, quit))

	consumer2 := NewKafkaConsumer(testTopic2, testGroupId2, brokers, nil)
	fmt.Printf("Trying to consume the message with Consumer 2 and group %s\n", testGroupId2)
	go consumer2.Read(readFunc(2, t, quit))
	time.Sleep(timeout * time.Second)
	t.Errorf("Failed to produce and consume a value within %d seconds", timeout)
	quit <- 1
	quit <- 1
}

func consumerGroupsRoutine(t *testing.T, quit chan int) {
	topic := uuid.New()
	consumerGroup1 := uuid.New()
//		consumerGroup2 := uuid.New()

	//create a new producer and send 2 messages to a random topic
	kafkaProducer := NewKafkaProducer(topic, brokers)
	fmt.Printf("Sending message 1 and 2 to topic %s\n", topic)
	kafkaProducer.Send("1")
	kafkaProducer.Send("2")

	//create a new consumer and try to consume the 2 produced messages
	consumer1 := NewKafkaConsumerGroup(topic, consumerGroup1, zookeeper, nil)
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
		time.Sleep(1 * time.Second)
	case <-time.After(timeout * time.Second):
		t.Errorf("Failed to consume messages with Consumer 1 within %d seconds", timeout)
	}

	//create one more consumer with the same consumer group and make sure messages are not consumed again
	consumer2 := NewKafkaConsumerGroup(topic, consumerGroup1, zookeeper, nil)
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
	for i := 3; i < 3 + numMessages; i++ {
		kafkaProducer.Send(uuid.New())
	}

	fmt.Println("consume these messages using 2 consumers with the same consumer groups")
	consumer3 := NewKafkaConsumerGroup(topic, consumerGroup1, zookeeper, nil)
	consumer4 := NewKafkaConsumerGroup(topic, consumerGroup1, zookeeper, nil)
	//total number of consumed messages should be 50 i.e. no duplicate or missing messages within one group
	messageCount2 := 0
//	waiter3 := make(chan int)
	writeFunc := func (consumerId int) func ([]byte) {
		return func (bytes []byte) {
			fmt.Printf("Consumer %d consumed message %s\n", consumerId, string(bytes))
			messageCount2++
//			time.Sleep(50 * time.Millisecond)
		}
	}
	go consumer3.Read(writeFunc(1))
	go consumer4.Read(writeFunc(2))

//	fmt.Println("wait a bit and check the number of consumed messages")
	<-time.After(timeout / 2 * time.Second)
	if (messageCount2 != numMessages) {
		t.Errorf("Invalid number of messages: expected %d, actual %d", numMessages, messageCount2)
	} else {
		fmt.Printf("Consumed %d messages\n", messageCount2)
	}

	//shutdown gracefully
	kafkaProducer.Close()
	consumer3.Close()
	consumer4.Close()
	quit <- 1
}

func readFunc(consumerId int, t *testing.T, quit chan int) func([]byte) {
	return func(bytes []byte) {
		message := string(bytes)
		if (message != testMessage) {
			t.Errorf("Produced value %s and consumed value %s do not match.", testMessage, message)
		} else {
			fmt.Printf("Consumer %d successfully consumed a message %s\n", consumerId, message)
		}
		quit <- 1
	}
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

func TestConsumerGroups(t * testing.T) {
	quit := make(chan int)

	go consumerGroupsRoutine(t, quit)
	<-quit
}
