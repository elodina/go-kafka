package mesos

//TODO not sure if consumer_factory is a good name for this

import kafka "github.com/stealthly/go_kafka_client"

// This function will be called each time the executor launches a new task.
// This is a place where you should provide your consumer behaviour, like fetch size, consumer strategy, timeouts, callbacks etc.
// ZookeeperConnect parameter specifies Zookeeper connection strings which should be set in the configuration.
func GetConsumer(ZookeeperConnect []string) *kafka.Consumer {
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = ZookeeperConnect

	config := kafka.DefaultConsumerConfig()
	config.AutoOffsetReset = kafka.SmallestOffset
	config.Coordinator = kafka.NewZookeeperCoordinator(zkConfig)

	config.Strategy = func(_ *kafka.Worker, msg *kafka.Message, id kafka.TaskId) kafka.WorkerResult {
		kafka.Debugf("Strategy", "Got message: %s\n", string(msg.Value))
		return kafka.NewSuccessfulResult(id)
	}

	config.WorkerFailedAttemptCallback = func(_ *kafka.Task, _ kafka.WorkerResult) kafka.FailedDecision {
		return kafka.CommitOffsetAndContinue
	}

	config.WorkerFailureCallback = func(_ *kafka.WorkerManager) kafka.FailedDecision {
		return kafka.DoNotCommitOffsetAndStop
	}

	return kafka.NewConsumer(config)
}
