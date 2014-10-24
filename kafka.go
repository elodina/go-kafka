package main

//import ("github.com/stealthly/go-kafka/producer"
//	"github.com/stealthly/go-kafka/consumer"
//	"fmt"
//	"time"
//)
//
//func main() {
//	var kafkaProducer = producer.NewKafkaProducer("test_topic1", []string{"go-broker:9092"}, nil)
//	kafkaProducer.Send("a message!")
//	kafkaProducer.Close()
//
//	kafkaConsumer := consumer.NewKafkaConsumer("test_topic1", "group1", []string{"go-broker:9092"}, nil)
//	go kafkaConsumer.Read(func(bytes []byte) {
//		message := string(bytes)
//		fmt.Println(message)
//	})
//
//	kafkaConsumerGroup := consumer.NewKafkaConsumerGroup("test_topic1", "group2", []string{"go-zookeeper:2181"}, nil)
//	go kafkaConsumerGroup.Read(func(bytes []byte) {
//		fmt.Printf("consumer group consumed %s\n", string(bytes))
//	})
//
//	time.Sleep(5 * time.Second)
//	fmt.Println("finished sleeping")
//	kafkaConsumer.Close()
//	kafkaConsumerGroup.Close()
//}
