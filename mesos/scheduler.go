package mesos

import (
	kafka "github.com/stealthly/go_kafka_client"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/golang/protobuf/proto"
	"fmt"
	"strings"
)

type SchedulerConfig struct {
	CpuPerTask         float64
	MemPerTask         float64
	Filter             kafka.TopicFilter
	Zookeeper          []string
	ArtifactServerPort int
	ExecutorBinaryName string
}

func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		CpuPerTask: 0.2,
		MemPerTask: 256,
	}
}

type Scheduler struct {
	Config *SchedulerConfig

	zookeeper *kafka.ZookeeperCoordinator
	consumerMap map[string]map[int32]*mesos.TaskID
}

func NewScheduler(config *SchedulerConfig) (*Scheduler, error) {
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = config.Zookeeper
	zookeeper := kafka.NewZookeeperCoordinator(zkConfig)
	if err := zookeeper.Connect(); err != nil {
		return nil, err
	}

	return &Scheduler{
		Config: config,
		zookeeper: zookeeper,
		consumerMap: make(map[string]map[int32]*mesos.TaskID),
	}, nil
}

func (this *Scheduler) String() string {
	return "Go Kafka Client Scheduler"
}

func (this *Scheduler) Registered(driver scheduler.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	kafka.Infof(this, "Framework Registered with Master %s", masterInfo)
}

func (this *Scheduler) Reregistered(driver scheduler.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	kafka.Infof(this, "Framework Re-Registered with Master %s", masterInfo)
}

func (this *Scheduler) Disconnected(scheduler.SchedulerDriver) {
	kafka.Info(this, "Disconnected")
}

func (this *Scheduler) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	kafka.Debugf(this, "Received offers: %s", offers)
	topicPartitions, err := this.getUnoccupiedTopicPartitions()
	if err != nil {
		kafka.Errorf(this, "Could not get topic-partitions to consume: %s", err)
		return
	}
	if len(topicPartitions) == 0 {
		kafka.Debugf(this, "There are no unoccupied topic-partitions, no need to start new consumers.")
		return
	}

	for _, offer := range offers {
		cpus := 0.0
		mems := 0.0

		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
				return res.GetName() == "cpus"
			})

		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
				return res.GetName() == "mem"
			})
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		kafka.Debugf(this, "Received Offer <%s> with cpus=%f, mem=%f", offer.Id.GetValue(), cpus, mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for len(topicPartitions) > 0 && this.Config.CpuPerTask <= remainingCpus && this.Config.MemPerTask <= remainingMems {
			topic, partition := this.takeTopicPartition(topicPartitions)
			taskId := &mesos.TaskID {
				Value: proto.String(fmt.Sprintf("%s-%d", topic, partition)),
			}

			task := &mesos.TaskInfo{
				Name:     proto.String(taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: this.createExecutorForTopicPartition(topic, partition),
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", float64(this.Config.CpuPerTask)),
					util.NewScalarResource("mem", float64(this.Config.MemPerTask)),
				},
			}
			kafka.Debugf(this, "Prepared task: %s with offer %s for launch", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= this.Config.CpuPerTask
			remainingMems -= this.Config.MemPerTask

			this.addConsumerForTopic(topic, partition, taskId)
		}
		kafka.Debugf(this, "Launching %d tasks for offer %s", len(tasks), offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (this *Scheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	kafka.Infof(this, "Status update: task %s is in state %s", status.TaskId.GetValue(), status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST || status.GetState() == mesos.TaskState_TASK_KILLED || status.GetState() == mesos.TaskState_TASK_FAILED {
		this.removeConsumerForTopic(status.GetTaskId())
	}
}

func (this *Scheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

func (this *Scheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}
func (this *Scheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}
func (this *Scheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (this *Scheduler) Error(driver scheduler.SchedulerDriver, err string) {
	kafka.Errorf(this, "Scheduler received error: %s", err)
}

func (this *Scheduler) getUnoccupiedTopicPartitions() (map[string][]int32, error) {
	topics, err := this.zookeeper.GetAllTopics()
	if err != nil {
		return nil, err
	}

	filteredTopics := make([]string, 0)
	for _, topic := range topics {
		if this.Config.Filter.TopicAllowed(topic, true) {
			filteredTopics = append(filteredTopics, topic)
		}
	}

	topicPartitions, err := this.zookeeper.GetPartitionsForTopics(filteredTopics)
	if err != nil {
		return nil, err
	}

	unoccupiedTopicPartitions := make(map[string][]int32)
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if this.consumerMap[topic] == nil || this.consumerMap[topic][partition] == nil {
				unoccupiedTopicPartitions[topic] = append(unoccupiedTopicPartitions[topic], partition)
			}
		}
	}

	copied := make(map[string][]int32)
	for k, v := range unoccupiedTopicPartitions {
		copied[k] = v
	}
	kafka.Debugf(this, "Unoccupied topic-partitions: %s", copied)
	return unoccupiedTopicPartitions, nil
}

func (this *Scheduler) createExecutorForTopicPartition(topic string, partition int32) *mesos.ExecutorInfo {
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("kafka-%s-%d", topic, partition)),
		Name:       proto.String("Go Kafka Client Executor"),
		Source:     proto.String("go-kafka"),
		Command: &mesos.CommandInfo{
			//TODO sudo chmod a+x is awful
			Value: proto.String(fmt.Sprintf("sudo chmod a+x %s && ./%s --zookeeper %s --topic %s --partition %d", this.Config.ExecutorBinaryName, this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), topic, partition)),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				//TODO fix master url
				Value: proto.String(fmt.Sprintf("http://master:%d/executor", this.Config.ArtifactServerPort)),
				Executable: proto.Bool(true),
			}},
		},
	}
}

func (this *Scheduler) takeTopicPartition(topicPartitions map[string][]int32) (string, int32) {
	for topic, partitions := range topicPartitions {
		topicPartitions[topic] = partitions[1:]

		if len(topicPartitions[topic]) == 0 {
			delete(topicPartitions, topic)
		}

		return topic, partitions[0]
	}

	panic("take on empty map")
}

func (this *Scheduler) addConsumerForTopic(topic string, partition int32, id *mesos.TaskID) {
	consumersForTopic := this.consumerMap[topic]
	if consumersForTopic == nil {
		this.consumerMap[topic] = make(map[int32]*mesos.TaskID)
		consumersForTopic = this.consumerMap[topic]
	}
	consumersForTopic[partition] = id
}

func (this *Scheduler) removeConsumerForTopic(id *mesos.TaskID) {
	for topic, partitions := range this.consumerMap {
		for partition, taskId := range partitions {
			if taskId.GetValue() == id.GetValue() {
				delete(this.consumerMap[topic], partition)
				return
			}
		}
	}

	kafka.Warn(this, "removeConsumerForTopic called for not existing TaskID")
}
