package consumer

import "github.com/wvanbergen/kafka/consumergroup"

// KafkaConsumerGroup operates on all partitions of a single topic.
// It uses Zookeeper to decide which consumer is consuming which partitions
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope.
type KafkaConsumerGroup struct {
	//the topic to consume from
	Topic            string

	//A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
	//group id multiple processes indicate that they are all part of the same consumer group.
	GroupId          string

	/* Zookeeper connection strings in form host:port */
	ZookeeperConnect []string
	consumer *consumergroup.ConsumerGroup
}

// Connects to a consumer group, using Zookeeper for auto-discovery
// You may also provide a consumergroup.ConsumerGroupConfig with more precise configurations or nil to use default configuration
func NewKafkaConsumerGroup(topic string, groupId string, zookeeper []string, config *consumergroup.ConsumerGroupConfig) *KafkaConsumerGroup {
	cons, consumerErr := consumergroup.JoinConsumerGroup(groupId, []string{topic}, zookeeper, config)
	if consumerErr != nil {
		panic(consumerErr)
	}

	return &KafkaConsumerGroup{ Topic: topic, GroupId: groupId, ZookeeperConnect: zookeeper, consumer: cons}
}

// Read starts consuming messages until the consumer is closed and applies the writeFunc to each consumed message.
// Note that this function blocks so should probably be used in a new go routine
func (consumerGroup *KafkaConsumerGroup) Read(writeFunc func(bytes []byte)) {
//	consumerGroup.consumer.
	stream := consumerGroup.consumer.Events()
	for {
		event, ok := <-stream
		if !ok {
			break
		}

		writeFunc(event.Value)
	}
}

// Close stops consuming messages and closes all underlying connections. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory.
func (consumerGroup *KafkaConsumerGroup) Close() {
	consumerGroup.consumer.Close()
}
