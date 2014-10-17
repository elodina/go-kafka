package main

import (
	"fmt"
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go-kafka/consumer"
	"time"
	"os"
	"os/signal"
	"github.com/stealthly/go-avro/decoder"
	"math/big"
)

type PingPong struct {
	Counter int64
	Name    string
}

var schemaRegistry = map[int64][]byte {
	int64(0): []byte("{\"type\":\"record\",\"name\":\"PingPong\",\"namespace\":\"scalago\",\"fields\":[{\"name\":\"counter\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}"),
}
var readTopic string
var writeTopic string
var group = "ping-pong-go-group"

var broker = "localhost:9092"
var zookeeper = "localhost:2181"

var kafkaProducer *producer.KafkaProducer = nil
var kafkaConsumer *consumer.KafkaConsumerGroup = nil

func main() {
	parseArgs()

	kafkaProducer = producer.NewKafkaProducer(writeTopic, []string{broker}, nil)
	kafkaConsumer = consumer.NewKafkaConsumerGroup(readTopic, group, []string{zookeeper}, nil)

	p := &PingPong{}
	pingPongLoop(p)
}

func parseArgs() {
	if len(os.Args) < 3 {
		panic("Usage: go run scala_go_kafka.go $READ_TOPIC $WRITE_TOPIC")
	}

	readTopic = os.Args[1]
	writeTopic = os.Args[2]
}

func pingPongLoop(p *PingPong) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Println("\ngolang > Closing consumer")
		kafkaConsumer.Close()
	}()

	fmt.Println("golang > Started!")
	kafkaConsumer.Read(func(bytes []byte) {
		time.Sleep(2 * time.Second)
		decode(p, bytes)
		fmt.Printf("golang > received %#v\n", p)
		modify(p)
		kafkaProducer.SendBytes(encode(p))
	})
}

func modify(obj *PingPong) {
	obj.Counter++
}

func encode(obj *PingPong) []byte {
	return []byte(fmt.Sprintf("%d", obj.Counter))
}

func decode(obj interface{}, bytes []byte) {
	dec := decoder.NewBinaryDecoder(bytes)
	dec.Seek(1)
	schemaIdArray := make([]byte, 4)
	dec.ReadFixed(schemaIdArray)

	schema := schemaById(schemaIdArray)
	datumReader := decoder.NewGenericDatumReader()
	datumReader.SetSchema(schema)

	datumReader.Read(obj, dec)
}

func schemaById(bytes []byte) *decoder.Schema {
	id := new (big.Int)
	id.SetBytes(bytes)
	schemaBytes := schemaRegistry[id.Int64()]
	return decoder.AvroSchema(schemaBytes)
}

//var CAMUS_MAGIC []byte = []byte {0}
//
//type CamusData struct {
//	dec *decoder.BinaryDecoder
//}
//
//func NewCamusData(data []byte) *CamusData {
//	dec := decoder.NewBinaryDecoder(data)
//	if magic, err := dec.ReadInt(); err != nil {
//		panic(err)
//	} else {
//		if byte(magic) != CAMUS_MAGIC {
//			panic("Wrong Camus magic byte")
//		}
//
//		schemaIdArray := make([]byte, 4)
//		dec.ReadFixed(schemaIdArray)
//	}
//}
