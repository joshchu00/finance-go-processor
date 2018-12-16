package twse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/decimal"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-protobuf"
	inf "gopkg.in/inf.v0"
)

var location *time.Location

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
}

type response struct {
	Stat  string     `json:"stat"`
	Data2 [][]string `json:"data2,omitempty"`
	Data4 [][]string `json:"data4,omitempty"`
	Data5 [][]string `json:"data5,omitempty"`
}

func Process(period string, ts int64, path string, last bool, firstDatetime int64, client *cassandra.Client, producer *kafka.Producer, topic string) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse process...", datetime.GetTimeString(ts, location)))

	var bytes []byte
	bytes, err = ioutil.ReadFile(path)
	if err != nil {
		return
	}

	res := &response{}
	err = json.Unmarshal(bytes, res)
	if err != nil {
		return
	}

	var data [][]string

	if res.Stat != "OK" {
		return
	} else if len(res.Data5) != 0 && len(res.Data5[0]) == 16 {
		data = res.Data5
	} else if len(res.Data4) != 0 && len(res.Data4[0]) == 16 {
		data = res.Data4
	} else if len(res.Data2) != 0 && len(res.Data2[0]) == 16 {
		data = res.Data2
	} else {
		err = errors.New("Unknown data format")
		return
	}

	for _, record := range data {

		symbol := record[0]
		name := record[1]

		if record[5] == "--" && record[6] == "--" && record[7] == "--" && record[8] == "--" {
			logger.Info(fmt.Sprintf("No record: %v", record))
			continue
		}

		if record[5] == "--" || record[6] == "--" || record[7] == "--" || record[8] == "--" {
			err = errors.New(fmt.Sprintf("No record: %v", record))
			return
		}

		record[4] = strings.Replace(record[4], ",", "", -1)
		record[5] = strings.Replace(record[5], ",", "", -1)
		record[6] = strings.Replace(record[6], ",", "", -1)
		record[7] = strings.Replace(record[7], ",", "", -1)
		record[8] = strings.Replace(record[8], ",", "", -1)

		var open, high, low, close *inf.Dec

		open, err = decimal.GetDecimal(record[5])
		if err != nil {
			return
		}

		high, err = decimal.GetDecimal(record[6])
		if err != nil {
			return
		}

		low, err = decimal.GetDecimal(record[7])
		if err != nil {
			return
		}

		close, err = decimal.GetDecimal(record[8])
		if err != nil {
			return
		}

		var volume int64
		volume, err = strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			return
		}

		client.InsertRecord(
			"TWSE",
			symbol,
			period,
			datetime.GetTime(ts, location),
			name,
			open,
			high,
			low,
			close,
			volume,
		)

		if last {
			message := &protobuf.Analyzer{
				Exchange: "TWSE",
				Symbol:   symbol,
				Period:   "1d",
				Datetime: firstDatetime,
			}

			bytes, err = proto.Marshal(message)
			if err != nil {
				return
			}

			producer.Produce(topic, 0, bytes)
		}
	}

	return
}
