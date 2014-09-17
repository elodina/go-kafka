package main

import (. "github.com/stealthly/go-kafka/producer"
	. "github.com/stealthly/go-kafka/consumer"
	"fmt"
	"time"
)

func main() {
	var kafkaProducer = NewKafkaProducer("test_topic", []string{"192.168.86.10:9092"})
	kafkaProducer.Send("a message!")
	kafkaProducer.Close()

	kafkaConsumer := NewKafkaConsumer("test_topic", "group1", []string{"192.168.86.10:9092"})
	go kafkaConsumer.Read(func(bytes []byte) {
		message := string(bytes)
		fmt.Println(message)
	})
	time.Sleep(5 * time.Second)
	fmt.Println("finished sleeping")
	kafkaConsumer.Close()
}
