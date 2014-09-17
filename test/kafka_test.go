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

	kafkaConsumer := NewKafkaConsumer(testTopic, testGroupId, brokers)
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

	consumer1 := NewKafkaConsumer(testTopic2, testGroupId, brokers)
	fmt.Printf("Trying to consume the message with Consumer 1 and group %s\n", testGroupId)
	go consumer1.Read(readFunc(1, t, quit))

	consumer2 := NewKafkaConsumer(testTopic2, testGroupId2, brokers)
	fmt.Printf("Trying to consume the message with Consumer 2 and group %s\n", testGroupId2)
	go consumer2.Read(readFunc(2, t, quit))
	time.Sleep(timeout * time.Second)
	t.Errorf("Failed to produce and consume a value within %d seconds", timeout)
	quit <- 1
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
