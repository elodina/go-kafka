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
	"code.google.com/p/go-uuid/uuid"
	"io/ioutil"
)

type PingPong struct {
	Counter int64
	Name    string
	Uuid	string
}

//custom string representation to match Scala version. just to simplify reading the console output
func (p *PingPong) String() string {
	return fmt.Sprintf("{\"counter\": %d, \"name\": \"%s\", \"uuid\": \"%s\"}", p.Counter, p.Name, p.Uuid)
}

var schemaRegistry = map[int64]string {
	int64(0): "./scalago.avsc",
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
		fmt.Printf("golang > received %v\n", p)
		modify(p)
		kafkaProducer.SendBytes(encode(p))
	})
}

func modify(obj *PingPong) {
	obj.Counter++
	obj.Uuid = uuid.New()
}

func encode(obj *PingPong) []byte {
	enc := decoder.NewBinaryEncoder()

	data := []byte {CAMUS_MAGIC}
	data = append(data, []byte{0x00, 0x00, 0x00, 0x00}...)
	data = append(data, enc.WriteLong(obj.Counter)...)
	data = append(data, enc.WriteString(obj.Name)...)
	data = append(data, enc.WriteString(obj.Uuid)...)
	return data
}

func decode(obj interface{}, bytes []byte) {
	NewCamusData(bytes).Read(obj)
}

func schemaById(bytes []byte) *decoder.Schema {
	id := new (big.Int)
	id.SetBytes(bytes)
	schemaFile := schemaRegistry[id.Int64()]
	if schemaBytes, err := ioutil.ReadFile(schemaFile); err != nil {
		panic(err)
	} else {
		return decoder.AvroSchema(schemaBytes)
	}
}

var CAMUS_MAGIC byte = byte(0)

type CamusData struct {
	dec *decoder.BinaryDecoder
	datumReader decoder.DatumReader
}

func NewCamusData(data []byte) *CamusData {
	dec := decoder.NewBinaryDecoder(data)
	if magic, err := dec.ReadInt(); err != nil {
		panic(err)
	} else {
		if byte(magic) != CAMUS_MAGIC {
			panic("Wrong Camus magic byte")
		}

		schemaIdArray := make([]byte, 4)
		dec.ReadFixed(schemaIdArray)
		schema := schemaById(schemaIdArray)
		datumReader := decoder.NewGenericDatumReader()
		datumReader.SetSchema(schema)

		return &CamusData{dec, datumReader}
	}
}

func (cd *CamusData) Read(obj interface{}) {
	cd.datumReader.Read(obj, cd.dec)
}
