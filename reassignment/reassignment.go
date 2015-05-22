/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"bufio"
	"encoding/json"
	"io/ioutil"
	"math"
	"os/exec"
	"strconv"
)

var kafkaPath = flag.String("kafka.path", "", "Absolute path to kafka installation")
var zookeeper = flag.String("zookeeper", "", "Zookeeper connection string")
var topic = flag.String("topic", "", "Topic name to reassign")
var partitions = flag.Int("partitions", 0, "Number of partitions to reassign")

var generate = flag.Bool("generate", false, "Flag to generate candidate assignment")
var execute = flag.Bool("execute", false, "Flag to execute reassignment")
var verify = flag.Bool("verify", false, "Flag to verify reassignment")

var brokerList = flag.String("broker.list", "", "Broker list")
var reassignmentFile = flag.String("reassignment", "", "Reassignment json file")

func parseAndValidateArgs() {
	flag.Parse()

    kafkaEnv := os.Getenv("KAFKA_PATH")
    if kafkaEnv != "" {
        *kafkaPath = kafkaEnv
    }

	if *kafkaPath == "" {
		fmt.Println("$KAFKA_PATH env or --kafka.path flag is required")
		os.Exit(1)
	}

	if strings.HasSuffix(*kafkaPath, "/") {
		*kafkaPath = (*kafkaPath)[:len(*kafkaPath)-1]
	}

	if *zookeeper == "" {
		fmt.Println("zookeeper flag is required")
		os.Exit(1)
	}

	if *generate {
        if *brokerList == "" {
            fmt.Println("broker.list flag is required for --generate flag.")
            os.Exit(1)
        }

        if *topic == "" {
            fmt.Println("topic flag is required for --generate flag.")
            os.Exit(1)
        }

        if *partitions == 0 {
            fmt.Println("At least one partition to reassign is required for --generate flag.")
            os.Exit(1)
        }
	}

	if (*execute || *verify) && *reassignmentFile == "" {
		fmt.Println("reassignment flag is required for --execute and --verify flags.")
		os.Exit(1)
	}
}

func main() {
	parseAndValidateArgs()

	if *generate {
		topicsJson := fmt.Sprintf(`{"topics": [{"topic": "%s"}],"version":1}`, *topic)
		topicsJsonFile := createTempFile("topics.json", topicsJson)

		generateCommand := fmt.Sprintf(`%s/bin/kafka-reassign-partitions.sh --zookeeper %s --topics-to-move-json-file %s --broker-list "%s" --generate`, *kafkaPath, *zookeeper, topicsJsonFile, *brokerList)
		generateResponse, err := exec.Command("sh", "-c", generateCommand).Output()
		if err != nil {
			panic(err)
		}

		parseGenerateResponse(generateResponse)

		os.RemoveAll(topicsJsonFile[:strings.LastIndex(topicsJsonFile, "/")])
	} else if *execute {
		executeReassignmentCandidate(*reassignmentFile)
	} else if *verify {
		verifyReassignment(*reassignmentFile)
	} else {
		fmt.Println("You should provide --generate, --execute or --verify flag")
		os.Exit(1)
	}
}

func parseGenerateResponse(bytes []byte) {
	lines := strings.Split(string(bytes), "\n")

	current := new(PartitionAssignments)
	err := json.Unmarshal([]byte(lines[2]), current)
	if err != nil {
		panic(err)
	}

	fmt.Println("Current partition replica assignment\n")
	indentedCurrent, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(indentedCurrent))

	candidate := createReassignmentCandidate(*current)
	if len(candidate.Partitions) == 0 {
		return
	}

	fmt.Println("\nProposed partition reassignment configuration\n")
	indentedCandidate, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(indentedCandidate))

	proposeExecuteCandidate(candidate)
}

func createReassignmentCandidate(current PartitionAssignments) *PartitionAssignments {
	replicaPartitionCount := make(map[int]int) //replicas to partitions counts, e.g. 1 has 23 partitions, 2 has 31 partitions etc.
	replicaPartitions := make(map[int][]int)   // replicas to partitions, e.g. 1 has partitions 1, 2, 3, 2 has 4, 5, 6 etc.
	partitionOwners := make(map[int][]int)     //partitions owners, e.g. partition 1 is owned by [1, 2], partition 2 by [2, 3] etc.

	brokers := strings.Split(*brokerList, ",")
	//initialize partitions counts as 0
	for _, broker := range brokers {
		id, err := strconv.Atoi(broker)
		if err != nil {
			panic(err)
		}
		replicaPartitionCount[id] = 0
	}

	for _, partitionInfo := range current.Partitions {
		for _, replica := range partitionInfo.Replicas {
			replicaPartitionCount[replica] = replicaPartitionCount[replica] + 1
			replicaPartitions[replica] = append(replicaPartitions[replica], partitionInfo.Partition)
		}

		replicas := make([]int, len(partitionInfo.Replicas))
		copy(replicas, partitionInfo.Replicas)
		partitionOwners[partitionInfo.Partition] = replicas
	}

	candidate := &PartitionAssignments{
		Version: 1,
	}

	for i := 0; i < *partitions; i++ {
		ok := movePartition(replicaPartitionCount, replicaPartitions, partitionOwners, candidate)
		if !ok {
			return candidate
		}
	}

	return candidate
}

func findReplicasWithMostPartitions(replicaPartitionCount map[int]int) []int {
	mostPartitions := -1
	ids := make([]int, 0)

	for replica, partitions := range replicaPartitionCount {
		if partitions > mostPartitions {
			mostPartitions = partitions
			ids = []int{replica}
		} else if partitions == mostPartitions {
			ids = append(ids, replica)
		}
	}

	return ids
}

func findReplicaWithLeastPartitions(replicaPartitionCount map[int]int) int {
	leastPartitions := math.MaxInt32
	id := -1

	for replica, partitions := range replicaPartitionCount {
		if partitions < leastPartitions {
			leastPartitions = partitions
			id = replica
		}
	}

	return id
}

func movePartition(replicaPartitionCount map[int]int, replicaPartitions map[int][]int, partitionOwners map[int][]int, candidate *PartitionAssignments) bool {
	if isBalanced(replicaPartitionCount) {
		fmt.Println("Cluster is already balanced, no need to move partitions anymore")
		return false
	}

	fromReplicas := findReplicasWithMostPartitions(replicaPartitionCount)
	to := findReplicaWithLeastPartitions(replicaPartitionCount)

	for _, from := range fromReplicas {
		partitionToMove := findPartitionToMove(from, to, replicaPartitions, partitionOwners)
		if partitionToMove == -1 {
			fmt.Printf("Replica %d does not have partitions that can be moved to replica %d", from, to)
			continue
		}

		replicaPartitions[to] = append(replicaPartitions[to], partitionToMove)

		replicaPartitionCount[from] = replicaPartitionCount[from] - 1
		replicaPartitionCount[to] = replicaPartitionCount[to] + 1

		for index, replica := range partitionOwners[partitionToMove] {
			if replica == from {
				partitionOwners[partitionToMove][index] = to
			}
		}

		// check whether the candidate already contains this partition not to duplicate it
		for _, partitionInfo := range candidate.Partitions {
			if partitionInfo.Partition == partitionToMove {
				return true
			}
		}

		candidate.Partitions = append(candidate.Partitions, &PartitionInfo{
			Topic:     *topic,
			Partition: partitionToMove,
			Replicas:  partitionOwners[partitionToMove],
		})

		return true
	}

	fmt.Println("Replicas %s don't have partitions that can be moved to %d", fromReplicas, to)
	return false
}

func findPartitionToMove(from int, to int, replicaPartitions map[int][]int, partitionOwners map[int][]int) int {
	for _, partition := range replicaPartitions[from] {
		contains := false
		for _, owner := range partitionOwners[partition] {
			if owner == to {
				contains = true
			}
		}

		if !contains {
			return partition
		}
	}

	return -1
}

func isBalanced(replicaPartitionCount map[int]int) bool {
	maxPartitions := -1
	for _, partitions := range replicaPartitionCount {
		maxPartitions = partitions
		break
	}

	for _, partitions := range replicaPartitionCount {
		if math.Abs(float64(maxPartitions-partitions)) > float64(1) {
			return false
		} else {
			if partitions > maxPartitions {
				maxPartitions = partitions
			}
		}
	}

	return true
}

func proposeExecuteCandidate(candidate *PartitionAssignments) {
	bytes, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nApply given configuration? ([y]es, [n]o, [s]ave to file): ")
	text, _ := reader.ReadString('\n')

	switch string(text[0]) {
	case "y":
		{
			newTopics := createTempFile("new_topics.json", string(bytes))
			executeReassignmentCandidate(newTopics)
		}
	case "n":
	case "s":
		{
			fmt.Print("Enter file name: ")
			fileName, _ := reader.ReadString('\n')
			if fileName == "\n" {
				proposeExecuteCandidate(candidate)
				return
			}

			err = ioutil.WriteFile(string(fileName[:len(fileName)-1]), bytes, 0700)
			if err != nil {
				panic(err)
			}
		}
	default:
		proposeExecuteCandidate(candidate)
	}
}

func executeReassignmentCandidate(file string) {
	executeCommand := fmt.Sprintf(`%s/bin/kafka-reassign-partitions.sh --zookeeper %s --reassignment-json-file %s --execute`, *kafkaPath, *zookeeper, file)
	executeResponse, err := exec.Command("sh", "-c", executeCommand).Output()
	if err != nil {
		panic(err)
	}

	fmt.Println(string(executeResponse))

	proposeVerifyReassignment(file)
}

func proposeVerifyReassignment(file string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nVerify given reassignment? ([y]es, [n]o): ")
	text, _ := reader.ReadString('\n')

	switch string(text[0]) {
	case "y":
		verifyReassignment(file)
	case "n":
	}
}

func verifyReassignment(file string) {
	verifyCommand := fmt.Sprintf(`%s/bin/kafka-reassign-partitions.sh --zookeeper %s --reassignment-json-file %s --verify`, *kafkaPath, *zookeeper, file)
	verifyResponse, err := exec.Command("sh", "-c", verifyCommand).Output()
	if err != nil {
		panic(err)
	}

	fmt.Println(string(verifyResponse))
}

func createTempFile(name string, contents string) string {
	tmpPath, err := ioutil.TempDir("", "reassignment")
	if err != nil {
		panic(err)
	}

	configPath := fmt.Sprintf("%s/%s", tmpPath, name)
	err = ioutil.WriteFile(configPath, []byte(contents), 0700)
	if err != nil {
		panic(err)
	}

	return configPath
}

type PartitionAssignments struct {
	Version    int              `json:"version"`
	Partitions []*PartitionInfo `json:"partitions"`
}

type PartitionInfo struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}
