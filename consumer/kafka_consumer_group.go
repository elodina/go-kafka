package consumer

import "github.com/wvanbergen/kafka/consumergroup"

type KafkaConsumerGroup struct {
	Topic            string
	GroupId          string
	ZookeeperConnect []string
	consumer *consumergroup.ConsumerGroup
}

func NewKafkaConsumerGroup(topic string, groupId string, zookeeper []string, config *consumergroup.ConsumerGroupConfig) *KafkaConsumerGroup {
	cons, consumerErr := consumergroup.JoinConsumerGroup(groupId, topic, zookeeper, config)
	if consumerErr != nil {
		panic(consumerErr)
	}

	return &KafkaConsumerGroup{ Topic: topic, GroupId: groupId, ZookeeperConnect: zookeeper, consumer: cons}
}

func (consumerGroup *KafkaConsumerGroup) Read(writeFunc func(bytes []byte)) {
	stream := consumerGroup.consumer.Stream()
	for {
		event, ok := <-stream
		if !ok {
			break
		}

		writeFunc(event.Value)
	}
}

func (consumerGroup *KafkaConsumerGroup) Close() {
	consumerGroup.consumer.Close()
}
