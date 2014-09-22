/* package consumer provides simplified usage of Kafka consumers and consumer groups build on top of https://github.com/Shopify/sarama and https://github.com/wvanbergen/kafka */
package consumer

import ("github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

// KafkaConsumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope.
type KafkaConsumer struct {
	//the topic to consume from
	Topic             string

	//A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
	//group id multiple processes indicate that they are all part of the same consumer group.
	GroupId           string

	/* This is for bootstrapping and the consumer will only use it for getting metadata (topics, partitions and replicas).
    The socket connections for getting the actual data will be established based on the broker information returned in
    the metadata. */
	BrokerList        []string
	quit              chan int
	client            *sarama.Client
	consumer        *sarama.Consumer
}

// NewKafkaConsumer creates a new consumer. It will read messages from the given topic, as part of the named consumer group.
// You may also provide a sarama.ConsumerConfig with more precise configurations or nil to use default configuration
func NewKafkaConsumer(topic string, groupId string, brokerList []string, config *sarama.ConsumerConfig) *KafkaConsumer {
	kafkaConsumer := &KafkaConsumer{Topic: topic, GroupId: groupId, BrokerList: brokerList}
	kafkaConsumer.quit = make(chan int)
	client, err := sarama.NewClient(uuid.New(), kafkaConsumer.BrokerList, nil)
	if err != nil {
		panic(err)
	}

	if config == nil {
		config = sarama.NewConsumerConfig()
	}
	consumer, err := sarama.NewConsumer(client, kafkaConsumer.Topic, 0, kafkaConsumer.GroupId, config)
	if err != nil {
		panic(err)
	}
	kafkaConsumer.client = client
	kafkaConsumer.consumer = consumer

	return kafkaConsumer
}

// Read starts consuming messages until the consumer is closed and applies the writeFunc to each consumed message.
// Note that this function blocks so should probably be used in a new go routine
func (kafkaConsumer *KafkaConsumer) Read(writeFunc func(bytes []byte)) {
readLoop:
	for {
		select {
		case event := <-kafkaConsumer.consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			writeFunc(event.Value)
		case <-kafkaConsumer.quit:
			break readLoop
		}
	}
}

// Close stops consuming messages and closes all underlying connections. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory.
func (kafkaConsumer KafkaConsumer) Close() {
	kafkaConsumer.quit <- 1
	kafkaConsumer.consumer.Close()
	kafkaConsumer.client.Close()
}
