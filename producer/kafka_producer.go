package producer

import (. "github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

type KafkaProducer struct {
	Topic      string
	BrokerList []string
	producer *Producer
}

func (kafkaProducer KafkaProducer) Send(message string) {
	if kafkaProducer.producer == nil {
		client, err := NewClient(uuid.New(), kafkaProducer.BrokerList, NewClientConfig())
		if err != nil {
			panic(err)
		}
		defer client.Close()

		producer, err := NewProducer(client, nil)
		if err != nil {
			panic(err)
		}
		defer producer.Close()
		kafkaProducer.producer = producer
	}

	kafkaProducer.producer.SendMessage(kafkaProducer.Topic, nil, StringEncoder(message))
}
