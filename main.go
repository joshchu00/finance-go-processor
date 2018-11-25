package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-protobuf"
	"github.com/joshchu00/finance-go-processor/twse"
	"github.com/spf13/viper"
)

func init() {

	// log
	logfile, err := os.OpenFile("logfile.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("FATAL", "Open log file error:", err)
	}

	log.SetOutput(logfile)
	log.SetPrefix("PROCESSOR ")
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	// config
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	viper.SetConfigName("config") // name of config file (without extension)
	// viper.AddConfigPath("/etc/appname/")   // path to look for the config file in
	// viper.AddConfigPath("$HOME/.appname")  // call multiple times to add many search paths
	viper.AddConfigPath(".")   // optionally look for config in the working directory
	err = viper.ReadInConfig() // Find and read the config file
	if err != nil {            // Handle errors reading the config file
		log.Fatalln("FATAL", "Open config file error:", err)
	}

	// log config
	log.Println("INFO", "environment:", viper.GetString("environment"))
	log.Println("INFO", "cassandra.hosts:", viper.GetString("cassandra.hosts"))
	log.Println("INFO", "cassandra.keyspace:", viper.GetString("cassandra.keyspace"))
	log.Println("INFO", "kafka.bootstrap.servers:", viper.GetString("kafka.bootstrap.servers"))
	log.Println("INFO", "kafka.topics.processor:", viper.GetString("kafka.topics.processor"))
	log.Println("INFO", "kafka.topics.analyzer:", viper.GetString("kafka.topics.analyzer"))
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

	// cassandra keyspace
	var cassandraKeyspace string
	cassandraKeyspace = fmt.Sprintf("%s_%s", viper.GetString("cassandra.keyspace"), environment)

	// cassandra client
	var cassandraClient *cassandra.Client
	if cassandraClient, err = cassandra.NewClient(viper.GetString("cassandra.hosts"), cassandraKeyspace); err != nil {
		return
	}
	defer cassandraClient.Close()

	// processor topic
	var processorTopic string
	processorTopic = fmt.Sprintf("%s_%s", viper.GetString("kafka.topics.processor"), environment)

	// processor consumer
	var processorConsumer *kafka.Consumer
	if processorConsumer, err = kafka.NewConsumer(viper.GetString("kafka.bootstrap.servers"), "processor", processorTopic); err != nil {
		return
	}
	defer processorConsumer.Close()

	// analyzer topic
	var analyzerTopic string
	analyzerTopic = fmt.Sprintf("%s_%s", viper.GetString("kafka.topics.analyzer"), environment)

	// analyzer producer
	var analyzerProducer *kafka.Producer
	if analyzerProducer, err = kafka.NewProducer(viper.GetString("kafka.bootstrap.servers")); err != nil {
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
			if err = twse.Process(message.Period, datetime.GetTime(message.Datetime), message.Path, message.IsFinished, cassandraClient, analyzerProducer, analyzerTopic); err != nil {
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
	environment = viper.GetString("environment")

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
