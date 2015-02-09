package mesos

import (
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	kafka "github.com/stealthly/go_kafka_client"
	"fmt"
)

type ExecutorConfig struct {
	Topic string
	Partition int32
	Filter kafka.TopicFilter
	Zookeeper []string
	Group string
}

func NewExecutorConfig() *ExecutorConfig {
	return &ExecutorConfig{}
}

type GoKafkaClientExecutor struct {
	Config *ExecutorConfig
	consumers map[string]*kafka.Consumer
}

func NewGoKafkaClientExecutor(config *ExecutorConfig) *GoKafkaClientExecutor {
	kafka.Logger = kafka.NewDefaultLogger(kafka.DebugLevel)

	return &GoKafkaClientExecutor{
		Config: config,
		consumers: make(map[string]*kafka.Consumer),
	}
}

func (this *GoKafkaClientExecutor) String() string {
	return fmt.Sprintf("Go Kafka Client Executor %s-%d", this.Config.Topic, this.Config.Partition)
}

func (this *GoKafkaClientExecutor) Registered(driver executor.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	kafka.Infof(this, "Registered Executor on slave %s", slaveInfo.GetHostname())
}

func (this *GoKafkaClientExecutor) Reregistered(driver executor.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	kafka.Infof(this, "Re-registered Executor on slave %s", slaveInfo.GetHostname())
}

func (this *GoKafkaClientExecutor) Disconnected(executor.ExecutorDriver) {
	kafka.Info(this, "Executor disconnected.")
}

func (this *GoKafkaClientExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	kafka.Infof(this, "Launching task %s with command %s", taskInfo.GetName(), taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		kafka.Errorf(this, "Failed to send status update: %s", runStatus)
	}

	taskId := taskInfo.GetTaskId().GetValue()

	consumer := this.createNewConsumer()
	if oldConsumer, exists := this.consumers[taskId]; exists {
		<-oldConsumer.Close()
	}
	this.consumers[taskId] = consumer
	go func() {
		if this.Config.Filter != nil {
			consumer.StartWildcard(this.Config.Filter, 1)
		} else {
			consumer.StartStaticPartitions(map[string][]int32 {this.Config.Topic : []int32{this.Config.Partition}})
		}

		// finish task
		kafka.Debugf(this, "Finishing task %s", taskInfo.GetName())
		finStatus := &mesos.TaskStatus{
			TaskId: taskInfo.GetTaskId(),
			State:  mesos.TaskState_TASK_FINISHED.Enum(),
		}
		if _, err := driver.SendStatusUpdate(finStatus); err != nil {
			kafka.Errorf(this, "Failed to send status update: %s", finStatus)
		}
		kafka.Infof(this, "Task %s has finished", taskInfo.GetName())
	}()
}

func (this *GoKafkaClientExecutor) KillTask(_ executor.ExecutorDriver, taskId *mesos.TaskID) {
	kafka.Info(this, "Kill task")

	this.closeConsumer(taskId.GetValue())
}

func (this *GoKafkaClientExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	kafka.Infof(this, "Got framework message: %s", msg)
}

func (this *GoKafkaClientExecutor) Shutdown(executor.ExecutorDriver) {
	kafka.Info(this, "Shutting down the executor")

	for taskId := range this.consumers {
		this.closeConsumer(taskId)
	}
}

func (this *GoKafkaClientExecutor) Error(driver executor.ExecutorDriver, err string) {
	kafka.Errorf(this, "Got error message: %s", err)
}

func (this *GoKafkaClientExecutor) closeConsumer(taskId string) {
	consumer, exists := this.consumers[taskId]
	if !exists {
		kafka.Warnf(this, "Got KillTask for unknown TaskID: %s", taskId)
		return
	}

	kafka.Debugf(this, "Closing consumer for TaskID %s", taskId)
	<-consumer.Close()
	kafka.Debugf(this, "Closed consumer for TaskID %s", taskId)
	delete(this.consumers, taskId)
}

func (this *GoKafkaClientExecutor) createNewConsumer() *kafka.Consumer {
	config := kafka.DefaultConsumerConfig()
	//TODO make ZookeeperCoordinator.config visible outside, so we can let user set his ZK settings AND still replace connection URLs
	SetupConsumerConfig(config)

	config.Groupid = this.Config.Group
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = this.Config.Zookeeper

	config.Coordinator = kafka.NewZookeeperCoordinator(zkConfig)


	return kafka.NewConsumer(config)
}
