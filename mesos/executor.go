// +build executor

package main

import (
	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"fmt"
	"time"
)

type GoKafkaClientExecutor struct {
	tasksLaunched int
}

func NewGoKafkaClientExecutor() *GoKafkaClientExecutor {
	return &GoKafkaClientExecutor{}
}

func (exec *GoKafkaClientExecutor) Registered(driver executor.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *GoKafkaClientExecutor) Reregistered(driver executor.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *GoKafkaClientExecutor) Disconnected(executor.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *GoKafkaClientExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	exec.tasksLaunched++
	fmt.Println("Total tasks launched ", exec.tasksLaunched)
	//
	// this is where one would perform the requested task
	//
	time.Sleep(60 * time.Second)

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

func (exec *GoKafkaClientExecutor) KillTask(executor.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *GoKafkaClientExecutor) FrameworkMessage(driver executor.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (exec *GoKafkaClientExecutor) Shutdown(executor.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *GoKafkaClientExecutor) Error(driver executor.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func main() {
	fmt.Println("Starting Go Kafka Client Executor")

	driver, err := executor.NewMesosExecutorDriver(NewGoKafkaClientExecutor())

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()
}
