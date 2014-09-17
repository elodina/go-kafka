package producer

import ("github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

type KafkaProducer struct {
	Topic      string
	BrokerList []string
	client *sarama.Client
	producer *sarama.Producer
}

func NewKafkaProducer(topic string, brokerList []string) *KafkaProducer {
	client, err := sarama.NewClient(uuid.New(), brokerList, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		panic(err)
	}

	return &KafkaProducer{topic, brokerList, client, producer}
}

func (kafkaProducer *KafkaProducer) Send(message string) error {
	return kafkaProducer.producer.SendMessage(kafkaProducer.Topic, nil, sarama.StringEncoder(message))
}

func (kafkaProducer *KafkaProducer) Close() {
	kafkaProducer.client.Close()
	kafkaProducer.producer.Close()
}
