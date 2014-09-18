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

	kafkaConsumer := NewKafkaConsumer("test_topic", "group1", []string{"192.168.86.10:9092"}, nil)
	go kafkaConsumer.Read(func(bytes []byte) {
		message := string(bytes)
		fmt.Println(message)
	})

	kafkaConsumerGroup := NewKafkaConsumerGroup("test_topic", "group2", []string{"192.168.86.5:2181"}, nil)
	go kafkaConsumerGroup.Read(func(bytes []byte) {
		fmt.Printf("consumer group consumed %s\n", string(bytes))
	})

	time.Sleep(5 * time.Second)
	fmt.Println("finished sleeping")
	kafkaConsumer.Close()
	kafkaConsumerGroup.Close()
}
