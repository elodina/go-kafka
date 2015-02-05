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
	topic          string
	partition      int32
	zookeeper      []string
	consumers map[string]*kafka.Consumer
}

func NewGoKafkaClientExecutor(zookeeper []string, topic string, partition int32) *GoKafkaClientExecutor {
	kafka.Logger = kafka.NewDefaultLogger(kafka.DebugLevel)

	return &GoKafkaClientExecutor{
		topic: topic,
		partition: partition,
		zookeeper: zookeeper,
		consumers: make(map[string]*kafka.Consumer),
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

	taskId := taskInfo.GetTaskId().GetValue()

	consumer := GetConsumer(this.zookeeper)
	if oldConsumer, exists := this.consumers[taskId]; exists {
		<-oldConsumer.Close()
	}
	this.consumers[taskId] = consumer
	//this is for test purposes
	go func() {
		kafka.Debug(this, "Started sleep routine")
		time.Sleep(time.Duration(rand.Intn(20) + 20) * time.Second)
		kafka.Debug(this, "Sleep finished, closing consumer")
		<-consumer.Close()
		kafka.Debug(this, "Close consumer finished")
	}()
	consumer.StartStaticPartitions(map[string][]int32 {this.topic : []int32{this.partition}})

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

func (this *GoKafkaClientExecutor) KillTask(_ executor.ExecutorDriver, taskId *mesos.TaskID) {
	kafka.Info(this, "Kill task")

	consumer, exists := this.consumers[taskId.GetValue()]
	if !exists {
		kafka.Warn(this, "Got KillTask for unknown TaskID")
		return
	}
	kafka.Debugf(this, "Closing consumer for TaskID %s", taskId.GetValue())
	<-consumer.Close()
	kafka.Debugf(this, "Closed consumer for TaskID %s", taskId.GetValue())
	delete(this.consumers, taskId.GetValue())
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
