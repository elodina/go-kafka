package producer

import (. "github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

type KafkaProducer struct {
	Topic      string
	BrokerList []string
	client *Client
	producer *Producer
}

func NewKafkaProducer(topic string, brokerList []string) *KafkaProducer {
	client, err := NewClient(uuid.New(), brokerList, NewClientConfig())
	if err != nil {
		panic(err)
	}

	producer, err := NewProducer(client, nil)
	if err != nil {
		panic(err)
	}

	return &KafkaProducer{topic, brokerList, client, producer}
}

func (kafkaProducer *KafkaProducer) Send(message string) error {
	return kafkaProducer.producer.SendMessage(kafkaProducer.Topic, nil, StringEncoder(message))
}

func (kafkaProducer *KafkaProducer) Close() {
	kafkaProducer.client.Close()
	kafkaProducer.producer.Close()
}
