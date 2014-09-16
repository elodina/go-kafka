package consumer

import ("github.com/Shopify/sarama"
	"code.google.com/p/go-uuid/uuid"
)

type KafkaConsumer struct {
	Topic             string
	GroupId           string
	BrokerList        []string
	quit              chan int
}

func NewKafkaConsumer(topic string, groupId string, brokerList []string) *KafkaConsumer {
	kafkaConsumer := &KafkaConsumer{Topic: topic, GroupId: groupId, BrokerList: brokerList}
	kafkaConsumer.quit = make(chan int)
	return kafkaConsumer
}

func (kafkaConsumer KafkaConsumer) Read(writeFunc func(bytes []byte)) {
	client, err := sarama.NewClient(uuid.New(), kafkaConsumer.BrokerList, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumer, err := sarama.NewConsumer(client, kafkaConsumer.Topic, 0, kafkaConsumer.GroupId, sarama.NewConsumerConfig())
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			writeFunc(event.Value)
		case <-kafkaConsumer.quit:
			return
		}
	}
}

func (kafkaConsumer KafkaConsumer) Close() {
	kafkaConsumer.quit <- 1
}
