package main

import (
	"fmt"
	"github.com/stealthly/go-kafka/producer"
	"github.com/stealthly/go-kafka/consumer"
	"github.com/stealthly/go-avro/decoder"
	"github.com/stealthly/go-avro/encoder"
	"github.com/stealthly/go-avro/schema"
	"github.com/stealthly/go-avro/avro"
	"time"
	"os"
	"os/signal"
	"math/big"
	"code.google.com/p/go-uuid/uuid"
	"io/ioutil"
	"bytes"
)

type PingPong struct {
	Counter int64
	Name    string
	Uuid    string
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

var broker = "go-broker:9092"
var zookeeper = "go-zookeeper:2181"

var kafkaProducer *producer.KafkaProducer = nil
var kafkaConsumer *consumer.KafkaConsumerGroup = nil

func main() {
	parseArgs()

	kafkaProducer = producer.NewKafkaProducer(writeTopic, []string{broker})
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
		camus := decode(p, bytes)
		fmt.Printf("golang > received %v\n", p)
		modify(p)
		if err := kafkaProducer.SendBytesSync(encode(p, camus.schemaId)); err != nil {
			panic(err)
		}
	})
}

func modify(obj *PingPong) {
	obj.Counter++
	obj.Uuid = uuid.New()
}

func encode(obj *PingPong, schemaId []byte) []byte {
	buffer := &bytes.Buffer{}
	buffer.Write([]byte {CAMUS_MAGIC})
	buffer.Write(schemaId)

	enc := encoder.NewBinaryEncoder(buffer)
	writer := encoder.NewGenericDatumWriter()
	writer.SetSchema(schemaById(schemaId))

	writer.Write(obj, enc)

	return buffer.Bytes()
}

func decode(obj interface{}, bytes []byte) *CamusData {
	camus := NewCamusData(bytes)
	camus.Read(obj)
	return camus
}

func schemaById(bytes []byte) schema.Schema {
	id := new(big.Int)
	id.SetBytes(bytes)
	schemaFile := schemaRegistry[id.Int64()]
	if schemaBytes, err := ioutil.ReadFile(schemaFile); err != nil {
		panic(err)
	} else {
		return schema.Parse(schemaBytes)
	}
}

var CAMUS_MAGIC byte = byte(0)

type CamusData struct {
	schemaId    []byte
	dec        *decoder.BinaryDecoder
	datumReader avro.DatumReader
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

		return &CamusData{ schemaIdArray, dec, datumReader }
	}
}

func (cd *CamusData) Read(obj interface{}) {
	cd.datumReader.Read(obj, cd.dec)
}
