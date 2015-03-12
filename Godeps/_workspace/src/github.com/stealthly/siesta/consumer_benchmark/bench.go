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
	"github.com/stealthly/siesta"
	"time"
//	"os"
//	"os/signal"
	"fmt"
	"github.com/Shopify/sarama"
)


func main() {
	topic := "siesta-1"
	partition := int32(0)
	seconds := 10

	testSiesta(topic, partition, seconds)
//	testSarama(topic, partition, seconds)
}

func testSiesta(topic string, partition int32, seconds int) {
	stop := false

	config := &siesta.ConnectorConfig{
		BrokerList:              []string{"localhost:9092"},
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		ConnectTimeout:          5 * time.Second,
		KeepAlive:               true,
		KeepAliveTimeout:        1 * time.Minute,
		MaxConnections:          5,
		MaxConnectionsPerBroker: 5,
		FetchSize:               500,
		ClientId:                "siesta",
	}

	connector := siesta.NewDefaultConnector(config)

	messageChannel := make(chan []*siesta.Message, 10000)
	count := 0
	go func() {
		for {
			messages := <-messageChannel
			count += len(messages)
		}
	}()

	//warm up
	fmt.Println("warming up")
	for i := 0; i < 5; i++ {
		connector.Consume(topic, partition, 0)
	}
	fmt.Println("warm up finished, starting")

	go func() {
		time.Sleep(time.Duration(seconds) * time.Second)
		stop = true
	}()

	offset := int64(0)
	for !stop {
		messages, err := connector.Consume(topic, partition, offset)
		if err != nil {
			panic(err)
		}
		messageChannel <- messages
		offset = messages[len(messages)-1].Offset
	}

	fmt.Printf("%d within %d secnods\n", count, seconds)
	fmt.Printf("%d average\n", count/seconds)
}

func testSarama(topic string, partition int32, seconds int) {
	stop := false

	config := sarama.NewClientConfig()
	client, err := sarama.NewClient("siesta", []string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	messageChannel := make(chan *sarama.MessageSet, 10000)
	count := 0
	go func() {
		for {
			set := <-messageChannel
			count += len(set.Messages)
		}
	}()

	broker, err := client.Leader(topic, partition)

	//warm up
	fmt.Println("warming up")
	for i := 0; i < 5; i++ {
		fetchRequest := new(sarama.FetchRequest)
		fetchRequest.MinBytes = 1
		fetchRequest.MaxWaitTime = 100
		fetchRequest.AddBlock(topic, partition, 0, 500)

		broker.Fetch("siesta", fetchRequest)
	}
	fmt.Println("warm up finished, starting")

	go func() {
		time.Sleep(time.Duration(seconds) * time.Second)
		stop = true
	}()

	offset := int64(0)
	if err != nil {
		panic(err)
	}
	for !stop {
		fetchRequest := new(sarama.FetchRequest)
		fetchRequest.MinBytes = 1
		fetchRequest.MaxWaitTime = 100
		fetchRequest.AddBlock(topic, partition, offset, 500)

		response, err := broker.Fetch("siesta", fetchRequest)
		if err != nil {
			panic(err)
		}
		set := response.Blocks[topic][partition].MsgSet
		messageChannel <- &set
		offset = set.Messages[len(set.Messages)-1].Offset
	}

	fmt.Printf("%d within %d secnods\n", count, seconds)
	fmt.Printf("%d average\n", count/seconds)
}
