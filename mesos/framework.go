// +build scheduler

package main

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/golang/protobuf/proto"
	"strconv"
	"net/http"
)

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
)

type Scheduler struct {
	executor *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks int
}

func NewScheduler(exec *mesos.ExecutorInfo) *Scheduler {
	return &Scheduler{
		exec, 0, 0, 1,
	}
}

func (sched *Scheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	println("Framework Registered with Master ", masterInfo)
}

func (sched *Scheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	println("Framework Re-Registered with Master ", masterInfo)
}

func (sched *Scheduler) Disconnected(scheduler.SchedulerDriver) {
	println("Disconnected")
}

func (sched *Scheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {

	for _, offer := range offers {
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
				return res.GetName() == "cpus"
			})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
				return res.GetName() == "mem"
			})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		println("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched < sched.totalTasks &&
			CPUS_PER_TASK <= remainingCpus &&
			MEM_PER_TASK <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUS_PER_TASK),
					util.NewScalarResource("mem", MEM_PER_TASK),
				},
			}
			println("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= CPUS_PER_TASK
			remainingMems -= MEM_PER_TASK
		}
		println("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *Scheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	println("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		println("Total tasks completed, stopping framework.")
		driver.Stop(false)
	}

	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		println(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)
		driver.Abort()
	}
}

func (sched *Scheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

func (sched *Scheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}
func (sched *Scheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}
func (sched *Scheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (sched *Scheduler) Error(driver scheduler.SchedulerDriver, err string) {
	println("Scheduler received error:", err)
}

func main() {
	go func() {
		http.HandleFunc("/executor", func(w http.ResponseWriter, r *http.Request) {
				http.ServeFile(w, r, "executor")
			})
		http.ListenAndServe(":6666", nil)
	}()

	executorInfo := &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command: &mesos.CommandInfo{
			Value: proto.String("sudo chmod a+rwx executor && ./executor"),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{Value: proto.String("http://master:6666/executor"), Executable: proto.Bool(true)}},
		},
	}

	frameworkInfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Go Kafka Client Framework"),
	}

	driver, err := scheduler.NewMesosSchedulerDriver(NewScheduler(executorInfo), frameworkInfo, "master:5050", nil)

	if err != nil {
		println("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		println("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}
}
