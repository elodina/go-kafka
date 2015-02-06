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
	GroupId            string
	ArtifactServerHost string
	ArtifactServerPort int
	ExecutorBinaryName string
	ExecutorArchiveName string
	KillTaskRetries int
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

	zookeeper *kafka.ZookeeperCoordinator
	consumerMap map[string]map[int32]*mesos.TaskID
}

func NewGoKafkaClientScheduler(config *SchedulerConfig) (*GoKafkaClientScheduler, error) {
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = config.Zookeeper
	zookeeper := kafka.NewZookeeperCoordinator(zkConfig)
	if err := zookeeper.Connect(); err != nil {
		return nil, err
	}

	return &GoKafkaClientScheduler{
		Config: config,
		zookeeper: zookeeper,
		consumerMap: make(map[string]map[int32]*mesos.TaskID),
	}, nil
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
	kafka.Debugf(this, "Received offers: %s", offers)
	topicPartitions, err := this.getUnoccupiedTopicPartitions()
	if err != nil {
		kafka.Errorf(this, "Could not get topic-partitions to consume: %s", err)
		this.declineOffers(driver, offers)
		return
	}
	if len(topicPartitions) == 0 {
		kafka.Debugf(this, "There are no unoccupied topic-partitions, no need to start new consumers.")
		this.declineOffers(driver, offers)
		return
	}

	for _, offer := range offers {
		cpus := this.getScalarResources(offer, "cpus")
		mems := this.getScalarResources(offer, "mem")

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

func (this *GoKafkaClientScheduler) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	kafka.Infof(this, "Status update: task %s is in state %s", status.TaskId.GetValue(), status.State.Enum().String())

	if status.GetState() == mesos.TaskState_TASK_LOST ||/* status.GetState() == mesos.TaskState_TASK_KILLED ||*/ status.GetState() == mesos.TaskState_TASK_FAILED || status.GetState() == mesos.TaskState_TASK_FINISHED {
		this.removeConsumerForTopic(status.GetTaskId())
	}
}

func (this *GoKafkaClientScheduler) OfferRescinded(scheduler.SchedulerDriver, *mesos.OfferID) {}

func (this *GoKafkaClientScheduler) FrameworkMessage(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}
func (this *GoKafkaClientScheduler) SlaveLost(scheduler.SchedulerDriver, *mesos.SlaveID) {}
func (this *GoKafkaClientScheduler) ExecutorLost(scheduler.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (this *GoKafkaClientScheduler) Error(driver scheduler.SchedulerDriver, err string) {
	kafka.Errorf(this, "Scheduler received error: %s", err)
}

func (this *GoKafkaClientScheduler) Shutdown(driver scheduler.SchedulerDriver) {
	kafka.Debug(this, "Shutting down scheduler.")
	for topic, partitions := range this.consumerMap {
		for partition, taskId := range partitions {
			if err := this.tryKillTask(driver, taskId, topic, partition); err != nil {
				kafka.Errorf(this, "Failed to kill task %s consuming topic %s and partition %d", taskId.GetValue(), topic, partition)
			}
		}
	}
}

func (this *GoKafkaClientScheduler) tryKillTask(driver scheduler.SchedulerDriver, taskId *mesos.TaskID, topic string, partition int32) error {
	kafka.Debugf(this, "Trying to kill task %s consuming topic %s and partition %d", taskId.GetValue(), topic, partition)

	var err error
	for i := 0; i <= this.Config.KillTaskRetries; i++ {
		if _, err = driver.KillTask(taskId); err == nil {
			return nil
		}
	}
	return err
}

func (this *GoKafkaClientScheduler) getUnoccupiedTopicPartitions() (map[string][]int32, error) {
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

	// copying can be removed after this is stable, but now we need this as the logging is async
	copied := make(map[string][]int32)
	for k, v := range unoccupiedTopicPartitions {
		copied[k] = v
	}
	kafka.Debugf(this, "Unoccupied topic-partitions: %s", copied)
	return unoccupiedTopicPartitions, nil
}

func (this *GoKafkaClientScheduler) createExecutorForTopicPartition(topic string, partition int32) *mesos.ExecutorInfo {
	path := strings.Split(this.Config.ExecutorArchiveName, "/")
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("kafka-%s-%d", topic, partition)),
		Name:       proto.String("Go Kafka Client Executor"),
		Source:     proto.String("go-kafka"),
		Command: &mesos.CommandInfo{
			Value: proto.String(fmt.Sprintf("./%s --zookeeper %s --group %s --topic %s --partition %d", this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), this.Config.GroupId, topic, partition)),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value: proto.String(fmt.Sprintf("http://%s:%d/%s", this.Config.ArtifactServerHost, this.Config.ArtifactServerPort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}},
		},
	}
}

func (this *GoKafkaClientScheduler) takeTopicPartition(topicPartitions map[string][]int32) (string, int32) {
	for topic, partitions := range topicPartitions {
		topicPartitions[topic] = partitions[1:]

		if len(topicPartitions[topic]) == 0 {
			delete(topicPartitions, topic)
		}

		return topic, partitions[0]
	}

	panic("take on empty map")
}

func (this *GoKafkaClientScheduler) addConsumerForTopic(topic string, partition int32, id *mesos.TaskID) {
	consumersForTopic := this.consumerMap[topic]
	if consumersForTopic == nil {
		this.consumerMap[topic] = make(map[int32]*mesos.TaskID)
		consumersForTopic = this.consumerMap[topic]
	}
	consumersForTopic[partition] = id
}

func (this *GoKafkaClientScheduler) removeConsumerForTopic(id *mesos.TaskID) {
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

func (this *GoKafkaClientScheduler) declineOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (this *GoKafkaClientScheduler) getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == resourceName
		})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}
