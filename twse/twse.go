package twse

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/decimal"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-protobuf"
	inf "gopkg.in/inf.v0"
)

type response struct {
	Data5 [][]string `json:"data5"`
}

func Process(period string, t time.Time, path string, isFinished bool, client *cassandra.Client, producer *kafka.Producer, topic string) (err error) {

	log.Println("INFO", "Starting twse process...", t.String())

	var bytes []byte
	if bytes, err = ioutil.ReadFile(path); err != nil {
		return
	}

	res := &response{}
	if err = json.Unmarshal(bytes, res); err != nil {
		return
	}

	for _, record := range res.Data5 {

		symbol := record[0]
		name := record[1]

		if record[5] == "--" && record[6] == "--" && record[7] == "--" && record[8] == "--" {
			log.Println("INFO", "No record", record)
			continue
		}

		if record[5] == "--" || record[6] == "--" || record[7] == "--" || record[8] == "--" {
			err = errors.New("No record")
			return
		}

		record[4] = strings.Replace(record[4], ",", "", -1)
		record[5] = strings.Replace(record[5], ",", "", -1)
		record[6] = strings.Replace(record[6], ",", "", -1)
		record[7] = strings.Replace(record[7], ",", "", -1)
		record[8] = strings.Replace(record[8], ",", "", -1)

		var open, high, low, close *inf.Dec

		if open, err = decimal.GetDecimal(record[5]); err != nil {
			return
		}

		if high, err = decimal.GetDecimal(record[6]); err != nil {
			return
		}

		if low, err = decimal.GetDecimal(record[7]); err != nil {
			return
		}

		if close, err = decimal.GetDecimal(record[8]); err != nil {
			return
		}

		var volume int64
		if volume, err = strconv.ParseInt(record[4], 10, 64); err != nil {
			return
		}

		client.InsertRecord(
			"TWSE",
			symbol,
			period,
			t,
			name,
			open,
			high,
			low,
			close,
			volume,
		)

		if isFinished {
			message := &protobuf.Analyzer{
				Exchange: "TWSE",
				Symbol:   symbol,
				Period:   "1d",
			}

			if bytes, err = proto.Marshal(message); err != nil {
				return
			}

			producer.Produce(topic, 0, bytes)
		}
	}

	return
}
