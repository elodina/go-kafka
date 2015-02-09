package mesos

import (
	kafka "github.com/stealthly/go_kafka_client"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/golang/protobuf/proto"
	"fmt"
	"strings"
)

type ConsumerTracker interface {
	CreateTasks(*mesos.Offer) []*mesos.TaskInfo
	TaskDied(*mesos.TaskID)
	GetAllTasks() []*mesos.TaskID
}

type StaticConsumerTracker struct {
	Config *SchedulerConfig
	zookeeper *kafka.ZookeeperCoordinator
	consumerMap map[string]map[int32]*mesos.TaskID
}

func NewStaticConsumerTracker(config *SchedulerConfig) (*StaticConsumerTracker, error) {
	zkConfig := kafka.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = config.Zookeeper
	zookeeper := kafka.NewZookeeperCoordinator(zkConfig)
	if err := zookeeper.Connect(); err != nil {
		return nil, err
	}

	return &StaticConsumerTracker{
		Config: config,
		zookeeper: zookeeper,
		consumerMap: make(map[string]map[int32]*mesos.TaskID),
	}, nil
}

func (this *StaticConsumerTracker) String() string {
	return "StaticConsumerTracker"
}

func (this *StaticConsumerTracker) CreateTasks(offer *mesos.Offer) []*mesos.TaskInfo {
	topicPartitions, err := this.getUnoccupiedTopicPartitions()
	if err != nil {
		kafka.Errorf(this, "Could not get topic-partitions to consume: %s", err)
		return nil
	}
	if len(topicPartitions) == 0 {
		kafka.Debugf(this, "There are no unoccupied topic-partitions, no need to start new consumers.")
		return nil
	}

	cpus := getScalarResources(offer, "cpus")
	mems := getScalarResources(offer, "mem")

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
	return tasks
}

func (this *StaticConsumerTracker) TaskDied(id *mesos.TaskID) {
	for topic, partitions := range this.consumerMap {
		for partition, taskId := range partitions {
			if taskId.GetValue() == id.GetValue() {
				delete(this.consumerMap[topic], partition)
				return
			}
		}
	}

	kafka.Warn(this, "TaskDied called for not existing TaskID")
}

func (this *StaticConsumerTracker) GetAllTasks() []*mesos.TaskID {
	ids := make([]*mesos.TaskID, 0)

	for _, partitions := range this.consumerMap {
		for _, taskId := range partitions {
			ids = append(ids, taskId)
		}
	}

	return ids
}

func (this *StaticConsumerTracker) getUnoccupiedTopicPartitions() (map[string][]int32, error) {
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

func (this *StaticConsumerTracker) takeTopicPartition(topicPartitions map[string][]int32) (string, int32) {
	for topic, partitions := range topicPartitions {
		topicPartitions[topic] = partitions[1:]

		if len(topicPartitions[topic]) == 0 {
			delete(topicPartitions, topic)
		}

		return topic, partitions[0]
	}

	panic("take on empty map")
}

func (this *StaticConsumerTracker) addConsumerForTopic(topic string, partition int32, id *mesos.TaskID) {
	consumersForTopic := this.consumerMap[topic]
	if consumersForTopic == nil {
		this.consumerMap[topic] = make(map[int32]*mesos.TaskID)
		consumersForTopic = this.consumerMap[topic]
	}
	consumersForTopic[partition] = id
}

func (this *StaticConsumerTracker) createExecutorForTopicPartition(topic string, partition int32) *mesos.ExecutorInfo {
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

type LoadBalancingConsumerTracker struct {
	Config *SchedulerConfig
	NumConsumers int
	aliveConsumers int
	consumerMap map[int]*mesos.TaskID
}

func NewLoadBalancingConsumerTracker(config *SchedulerConfig, numConsumers int) *LoadBalancingConsumerTracker {
	return &LoadBalancingConsumerTracker{
		Config: config,
		NumConsumers: numConsumers,
		consumerMap: make(map[int]*mesos.TaskID),
	}
}

func (this *LoadBalancingConsumerTracker) String() string {
	return "LoadBalancingConsumerTracker"
}

func (this *LoadBalancingConsumerTracker) CreateTasks(offer *mesos.Offer) []*mesos.TaskInfo {
	cpus := getScalarResources(offer, "cpus")
	mems := getScalarResources(offer, "mem")

	kafka.Debugf(this, "Received Offer <%s> with cpus=%f, mem=%f", offer.Id.GetValue(), cpus, mems)

	remainingCpus := cpus
	remainingMems := mems

	var tasks []*mesos.TaskInfo
	id := this.getFreeId()
	for id > -1 && this.Config.CpuPerTask <= remainingCpus && this.Config.MemPerTask <= remainingMems {
		taskId := &mesos.TaskID {
			Value: proto.String(fmt.Sprintf("go-kafka-%d", id)),
		}

		task := &mesos.TaskInfo{
			Name:     proto.String(taskId.GetValue()),
			TaskId:   taskId,
			SlaveId:  offer.SlaveId,
			Executor: this.createExecutor(id),
			Resources: []*mesos.Resource{
				util.NewScalarResource("cpus", float64(this.Config.CpuPerTask)),
				util.NewScalarResource("mem", float64(this.Config.MemPerTask)),
			},
		}
		kafka.Debugf(this, "Prepared task: %s with offer %s for launch", task.GetName(), offer.Id.GetValue())

		tasks = append(tasks, task)
		remainingCpus -= this.Config.CpuPerTask
		remainingMems -= this.Config.MemPerTask

		this.consumerMap[id] = taskId
		id = this.getFreeId()
	}
	kafka.Debugf(this, "Launching %d tasks for offer %s", len(tasks), offer.Id.GetValue())
	return tasks
}

func (this *LoadBalancingConsumerTracker) TaskDied(diedId *mesos.TaskID) {
	for id, taskId := range this.consumerMap {
		if taskId.GetValue() == diedId.GetValue() {
			delete(this.consumerMap, id)
			return
		}
	}

	kafka.Warn(this, "TaskDied called for not existing TaskID")
}

func (this *LoadBalancingConsumerTracker) GetAllTasks() []*mesos.TaskID {
	ids := make([]*mesos.TaskID, 0)

	for _, taskId := range this.consumerMap {
		ids = append(ids, taskId)
	}

	return ids
}

func (this *LoadBalancingConsumerTracker) getFreeId() int {
	for id := 0; id < this.NumConsumers; id++ {
		if _, exists := this.consumerMap[id]; !exists {
			return id
		}
	}

	return -1
}

func (this *LoadBalancingConsumerTracker) createExecutor(id int) *mesos.ExecutorInfo {
	path := strings.Split(this.Config.ExecutorArchiveName, "/")
	var command string
	if this.Config.Whitelist != "" {
		command = fmt.Sprintf("./%s --zookeeper %s --group %s --whitelist %s", this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), this.Config.GroupId, this.Config.Whitelist)
	} else {
		command = fmt.Sprintf("./%s --zookeeper %s --group %s --blacklist %s", this.Config.ExecutorBinaryName, strings.Join(this.Config.Zookeeper, ","), this.Config.GroupId, this.Config.Blacklist)
	}
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID(fmt.Sprintf("kafka-%d", id)),
		Name:       proto.String("Go Kafka Client Executor"),
		Source:     proto.String("go-kafka"),
		Command: &mesos.CommandInfo{
			Value: proto.String(command),
			Uris:  []*mesos.CommandInfo_URI{&mesos.CommandInfo_URI{
				Value: proto.String(fmt.Sprintf("http://%s:%d/%s", this.Config.ArtifactServerHost, this.Config.ArtifactServerPort, path[len(path)-1])),
				Extract: proto.Bool(true),
			}},
		},
	}
}

func getScalarResources(offer *mesos.Offer, resourceName string) float64 {
	resources := 0.0
	filteredResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == resourceName
		})
	for _, res := range filteredResources {
		resources += res.GetScalar().GetValue()
	}
	return resources
}
