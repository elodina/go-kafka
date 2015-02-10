package mesos

import (
	kafka "github.com/stealthly/go_kafka_client"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/golang/protobuf/proto"
)

type SchedulerConfig struct {
	CpuPerTask          float64
	MemPerTask          float64
	Filter              kafka.TopicFilter
	Zookeeper           []string
	GroupId             string
	ArtifactServerHost  string
	ArtifactServerPort  int
	ExecutorBinaryName  string
	ExecutorArchiveName string
	KillTaskRetries     int
	Static              bool
	NumConsumers        int
}

func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		CpuPerTask: 0.2,
		MemPerTask: 256,
		KillTaskRetries: 3,
	}
}

type GoKafkaClientScheduler struct {
	Config *SchedulerConfig
	Tracker ConsumerTracker
}

func NewGoKafkaClientScheduler(config *SchedulerConfig, tracker ConsumerTracker) *GoKafkaClientScheduler {
	return &GoKafkaClientScheduler{
		Config: config,
		Tracker: tracker,
	}
}

func (this *GoKafkaClientScheduler) String() string {
	return "Go Kafka Client Scheduler"
}

func (this *GoKafkaClientScheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	kafka.Infof(this, "Framework Registered with Master %s", masterInfo)
}

func (this *GoKafkaClientScheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	kafka.Infof(this, "Framework Re-Registered with Master %s", masterInfo)
}

func (this *GoKafkaClientScheduler) Disconnected(scheduler.SchedulerDriver) {
	kafka.Info(this, "Disconnected")
}

func (this *GoKafkaClientScheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	kafka.Tracef(this, "Received offers: %s", offers)

	for _, offer := range offers {
		tasks := this.Tracker.CreateTasks(offer)
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (this *GoKafkaClientScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	kafka.Infof(this, "Status update: task %s is in state %s", status.TaskId.GetValue(), status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_FINISHED {
		this.Tracker.TaskDied(status.GetTaskId())
	}
}

func (this *GoKafkaClientScheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

func (this *GoKafkaClientScheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}

//TODO probably will have to implement these 2 methods as well to handle outages
func (this *GoKafkaClientScheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}
func (this *GoKafkaClientScheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (this *GoKafkaClientScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	kafka.Errorf(this, "Scheduler received error: %s", err)
}

func (this *GoKafkaClientScheduler) Shutdown(driver scheduler.SchedulerDriver) {
	kafka.Debug(this, "Shutting down scheduler.")
	for _, taskId := range this.Tracker.GetAllTasks() {
		if err := this.tryKillTask(driver, taskId); err != nil {
			kafka.Errorf(this, "Failed to kill task %s", taskId.GetValue())
		}
	}
}

func (this *GoKafkaClientScheduler) tryKillTask(driver scheduler.SchedulerDriver, taskId *mesos.TaskID) error {
	kafka.Debugf(this, "Trying to kill task %s", taskId.GetValue())

	var err error
	for i := 0; i <= this.Config.KillTaskRetries; i++ {
		if _, err = driver.KillTask(taskId); err == nil {
			return nil
		}
	}
	return err
}
