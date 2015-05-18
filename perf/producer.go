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
	"fmt"
	"github.com/stealthly/go-kafka/perf/avro"
	kafka "github.com/stealthly/go_kafka_client"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	NS_PER_MS    int64 = 1000000
	NS_PER_SEC         = 1000 * NS_PER_MS
	MIN_SLEEP_NS       = 2 * NS_PER_MS
)

func main() {
	args := os.Args
	if len(args) != 7 {
		fmt.Println("USAGE: producer topic_name num_records record_size target_records_sec broker_list schema_registry")
		os.Exit(1)
	}

	topicName := args[1]
	numRecords, err := strconv.Atoi(args[2])
	if err != nil {
		panic(err)
	}
	recordSize, err := strconv.Atoi(args[3])
	if err != nil {
		panic(err)
	}
	throughput, err := strconv.Atoi(args[4])
	if err != nil {
		panic(err)
	}
	brokerList := args[5]
	schemaRegistry := args[6]

	producerConfig := kafka.DefaultProducerConfig()
	producerConfig.BrokerList = strings.Split(brokerList, ",")
	producerConfig.ValueEncoder = kafka.NewKafkaAvroEncoder(schemaRegistry)

	producer := kafka.NewSaramaProducer(producerConfig)
	input := producer.Input()

	sleepTime := NS_PER_SEC / int64(throughput)
	sleepDeficitNs := int64(0)

	for i := 0; i != numRecords; i++ {
		record := randomLogLine(topicName, recordSize)
		input <- &kafka.ProducerMessage{Topic: topicName, Value: record}

		if throughput > 0 {
			sleepDeficitNs += sleepTime
			if sleepDeficitNs >= MIN_SLEEP_NS {
				sleepMs := sleepDeficitNs / 1000000
				sleepNs := sleepDeficitNs - sleepMs*1000000
				time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				time.Sleep(time.Duration(sleepNs) * time.Nanosecond)
				sleepDeficitNs = 0
			}
		}
	}

	producer.Close()
}

func randomLogLine(topicName string, recordSize int) *avro.LogLine {
	timestamp := int64(time.Now().UnixNano() / int64(time.Millisecond))

	logLine := avro.NewLogLine()
	logLine.Line = randString(recordSize)
	logLine.Source = "perf-producer"
	logLine.Timings = []*avro.KV{&avro.KV{"generated", timestamp}}

	return logLine
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
