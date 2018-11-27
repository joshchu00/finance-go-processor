package main

import (
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-processor/twse"
	"github.com/joshchu00/finance-protobuf"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "processor")

	// log config
	log.Println("INFO", "Environment:", config.Environment())
	log.Println("INFO", "CassandraHosts:", config.CassandraHosts())
	log.Println("INFO", "CassandraKeyspace:", config.CassandraKeyspace())
	log.Println("INFO", "KafkaBootstrapServers:", config.KafkaBootstrapServers())
	log.Println("INFO", "KafkaProcessorTopic:", config.KafkaProcessorTopic())
	log.Println("INFO", "KafkaAnalyzerTopic:", config.KafkaAnalyzerTopic())
}

var environment string

func process() {

	if environment == "prod" {
		defer func() {
			if err := recover(); err != nil {
				log.Println("PANIC", "recover", err)
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	if cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace()); err != nil {
		return
	}
	defer cassandraClient.Close()

	// processor consumer
	var processorConsumer *kafka.Consumer
	if processorConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "processor", config.KafkaProcessorTopic()); err != nil {
		return
	}
	defer processorConsumer.Close()

	// analyzer producer
	var analyzerProducer *kafka.Producer
	if analyzerProducer, err = kafka.NewProducer(config.KafkaBootstrapServers()); err != nil {
		return
	}
	defer analyzerProducer.Close()

	for {

		message := &protobuf.Processor{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		if topic, partition, offset, value, err = processorConsumer.Consume(); err != nil {
			log.Panicln("PANIC", "Consume", err)
		}

		if err = proto.Unmarshal(value, message); err != nil {
			log.Panicln("PANIC", "Unmarshal", err)
		}

		switch message.Exchange {
		case "TWSE":
			if err = twse.Process(message.Period, datetime.GetTime(message.Datetime), message.Path, message.IsFinished, cassandraClient, analyzerProducer, config.KafkaAnalyzerTopic()); err != nil {
				log.Panicln("PANIC", "Process", err)
			}
		default:
			log.Panicln("PANIC", "Unknown exchange")
		}

		// strange
		offset++

		if err = processorConsumer.CommitOffset(topic, partition, offset); err != nil {
			log.Panicln(err)
		}
	}
}

func main() {

	log.Println("INFO", "Starting processor...")

	// environment
	environment = config.Environment()

	if environment != "dev" && environment != "test" && environment != "stg" && environment != "prod" {
		log.Panicln("PANIC", "Unknown environment")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environment != "prod" {
			break
		}
	}
}
