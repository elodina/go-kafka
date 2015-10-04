/* package producer provides simplified usage of Kafka producers build on top of https://github.com/Shopify/sarama */
package producer

import (
	"github.com/Shopify/sarama"
)

// KafkaProducer publishes Kafka messages to a given topic.
// You MUST call Close() on a producer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope.
type KafkaProducer struct {
	//the topic to consume from
	Topic string

	/* This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas).
	   The socket connections for sending the actual data will be established based on the broker information returned in
	   the metadata. */
	BrokerList []string
	client     *sarama.Client
	producer   *sarama.Producer
}

// NewKafkaProducer creates a new produce. It will publish messages to the given topic.
// You may also provide a sarama.ProducerConfig with more precise configurations or nil to use default configuration
func NewKafkaProducer(topic string, brokerList []string) *KafkaProducer {
	client, err := sarama.NewClient(uuid.New(), brokerList, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	}

	config := sarama.NewProducerConfig()
	config.FlushMsgCount = 1
	config.AckSuccesses = true
	producer, err := sarama.NewProducer(client, config)
	if err != nil {
		panic(err)
	}

	return &KafkaProducer{topic, brokerList, client, producer}
}

func (kafkaProducer *KafkaProducer) SendStringSync(message string) error {
	return kafkaProducer.sendSync(sarama.StringEncoder(message))
}

func (kafkaProducer *KafkaProducer) SendBytesSync(message []byte) error {
	return kafkaProducer.sendSync(sarama.ByteEncoder(message))
}

func (kafkaProducer *KafkaProducer) sendSync(encoder sarama.Encoder) error {
	message := &sarama.ProducerMessage{Topic: kafkaProducer.Topic, Key: nil, Value: encoder}
	kafkaProducer.producer.Input() <- message
	select {
	case error := <-kafkaProducer.producer.Errors():
		return error.Err
	case <-kafkaProducer.producer.Successes():
		return nil
	}
}

// Close indicates that no more messages will be produced with this producer and closes all underlying connections. It is required to call this function before
// a producer object passes out of scope, as it will otherwise leak memory.
func (kafkaProducer *KafkaProducer) Close() {
	kafkaProducer.producer.Close()
	kafkaProducer.client.Close()
}
