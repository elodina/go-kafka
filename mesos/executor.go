package mesos

import (
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"fmt"
	kafka "github.com/stealthly/go_kafka_client"
)

type GoKafkaClientExecutor struct {
	tasksLaunched int
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
		fmt.Printf("Got message: %s\n", string(msg.Value))
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

func (this *GoKafkaClientExecutor) Registered(driver executor.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (this *GoKafkaClientExecutor) Reregistered(driver executor.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (this *GoKafkaClientExecutor) Disconnected(executor.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (this *GoKafkaClientExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	this.tasksLaunched++
	fmt.Println("Total tasks launched ", this.tasksLaunched)
	//this is for test purposes
	//	go func() {
	//		fmt.Println("Started sleep routine")
	//		time.Sleep(time.Duration(rand.Intn(20) + 20) * time.Second)
	//		fmt.Println("Sleep finished, closing consumer")
	//		<-exec.consumer.Close()
	//		fmt.Println("Close consumer finished")
	//	}()
	this.consumer.StartStaticPartitions(map[string][]int32 {this.topic : []int32{this.partition}})

	// finish task
	fmt.Println("Finishing task", taskInfo.GetName())
	finStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_FINISHED.Enum(),
	}
	_, err = driver.SendStatusUpdate(finStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}
	fmt.Println("Task finished", taskInfo.GetName())
}

func (this *GoKafkaClientExecutor) KillTask(executor.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (this *GoKafkaClientExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (this *GoKafkaClientExecutor) Shutdown(executor.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (this *GoKafkaClientExecutor) Error(driver executor.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}
