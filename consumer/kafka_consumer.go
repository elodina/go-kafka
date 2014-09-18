package consumer

import ("github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

type KafkaConsumer struct {
	Topic             string
	GroupId           string
	BrokerList        []string
	quit              chan int
	client            *sarama.Client
	consumer        *sarama.Consumer
}

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

func (kafkaConsumer KafkaConsumer) Close() {
	kafkaConsumer.quit <- 1
	kafkaConsumer.client.Close()
	kafkaConsumer.consumer.Close()
}
