package main

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-processor/twse"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "processor")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "EnvironmentName", config.EnvironmentName()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "DataDirectory", config.DataDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraHosts", config.CassandraHosts()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraKeyspace", config.CassandraKeyspace()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaProcessorTopic", config.KafkaProcessorTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaAnalyzerTopic", config.KafkaAnalyzerTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "TWSEDataDirectory", config.TWSEDataDirectory()))
}

var environmentName string

func process() {

	if environmentName == config.EnvironmentNameProd {
		defer func() {
			if err := recover(); err != nil {
				logger.Panic(fmt.Sprintf("recover %v", err))
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace())
	if err != nil {
		logger.Panic(fmt.Sprintf("cassandra.NewClient %v", err))
	}
	defer cassandraClient.Close()

	// processor consumer
	var processorConsumer *kafka.Consumer
	processorConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "processor", config.KafkaProcessorTopic())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewConsumer %v", err))
	}
	defer processorConsumer.Close()

	// analyzer producer
	var analyzerProducer *kafka.Producer
	analyzerProducer, err = kafka.NewProducer(config.KafkaBootstrapServers())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewProducer %v", err))
	}
	defer analyzerProducer.Close()

	for {

		message := &protobuf.Processor{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		topic, partition, offset, value, err = processorConsumer.Consume()
		if err != nil {
			logger.Panic(fmt.Sprintf("Consume %v", err))
		}

		err = proto.Unmarshal(value, message)
		if err != nil {
			logger.Panic(fmt.Sprintf("proto.Unmarshal %v", err))
		}

		switch message.Exchange {
		case "TWSE":
			err = twse.Process(
				message.Period,
				message.StartDatetime,
				message.EndDatetime,
				config.TWSEDataDirectory(),
				cassandraClient,
				analyzerProducer,
				config.KafkaAnalyzerTopic(),
			)
			if err != nil {
				logger.Panic(fmt.Sprintf("Process %v", err))
			}
		default:
			logger.Panic("Unknown exchange")
		}

		// strange
		offset++

		err = processorConsumer.CommitOffset(topic, partition, offset)
		if err != nil {
			logger.Panic(fmt.Sprintf("CommitOffset %v", err))
		}
	}
}

func main() {

	logger.Info("Starting processor...")

	// environment name
	switch environmentName = config.EnvironmentName(); environmentName {
	case config.EnvironmentNameDev, config.EnvironmentNameTest, config.EnvironmentNameStg, config.EnvironmentNameProd:
	default:
		logger.Panic("Unknown environment name")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environmentName != config.EnvironmentNameProd {
			break
		}
	}
}
