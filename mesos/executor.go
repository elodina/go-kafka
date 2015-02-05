package mesos

import (
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	kafka "github.com/stealthly/go_kafka_client"
	"fmt"
	"time"
	"math/rand"
)

type GoKafkaClientExecutor struct {
	topic         string
	partition     int32
	consumer *kafka.Consumer
}

func NewGoKafkaClientExecutor(zookeeper []string, topic string, partition int32) *GoKafkaClientExecutor {
	kafka.Logger = kafka.NewDefaultLogger(kafka.DebugLevel)
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = zookeeper
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
	consumer := kafka.NewConsumer(config)
	return &GoKafkaClientExecutor{
		topic: topic,
		partition: partition,
		consumer: consumer,
	}
}

func (this *GoKafkaClientExecutor) String() string {
	return fmt.Sprintf("Go Kafka Client Executor %s-%d", this.topic, this.partition)
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

	//this is for test purposes
	go func() {
		kafka.Debug(this, "Started sleep routine")
		time.Sleep(time.Duration(rand.Intn(20) + 20) * time.Second)
		kafka.Debug(this, "Sleep finished, closing consumer")
		<-this.consumer.Close()
		kafka.Debug(this, "Close consumer finished")
	}()
	this.consumer.StartStaticPartitions(map[string][]int32 {this.topic : []int32{this.partition}})

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
}

//TODO implement
func (this *GoKafkaClientExecutor) KillTask(executor.ExecutorDriver, *mesos.TaskID) {
	kafka.Info(this, "Kill task")
}

func (this *GoKafkaClientExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	kafka.Infof(this, "Got framework message: %s", msg)
}

func (this *GoKafkaClientExecutor) Shutdown(executor.ExecutorDriver) {
	kafka.Info(this, "Shutting down the executor")
}

func (this *GoKafkaClientExecutor) Error(driver executor.ExecutorDriver, err string) {
	kafka.Errorf(this, "Got error message: %s", err)
}
