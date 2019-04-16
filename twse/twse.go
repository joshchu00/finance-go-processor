package twse

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/data"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/decimal"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
	inf "gopkg.in/inf.v0"
)

type response struct {
	Stat  string     `json:"stat"`
	Data2 [][]string `json:"data2,omitempty"`
	Data4 [][]string `json:"data4,omitempty"`
	Data5 [][]string `json:"data5,omitempty"`
}

func Process(
	period string,
	start int64,
	end int64,
	dataDirectory string,
	client *cassandra.Client,
	producer *kafka.Producer,
	topic string,
) (err error) {

	logger.Info("Starting twse process...")

	var location *time.Location
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		return
	}

	logger.Debug(fmt.Sprintf("%d %d %s", start, end, dataDirectory))

	symbols := make(map[string]int64)

	for ts := start; ts <= end; ts = datetime.AddOneDay(ts) {

		dateString := datetime.GetDateString(ts, location)
		path := data.GetPath(dataDirectory, dateString)

		logger.Debug(path)

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

		var records [][]string

		if res.Stat != "OK" {
			return
		} else if len(res.Data5) != 0 && len(res.Data5[0]) == 16 {
			records = res.Data5
		} else if len(res.Data4) != 0 && len(res.Data4[0]) == 16 {
			records = res.Data4
		} else if len(res.Data2) != 0 && len(res.Data2[0]) == 16 {
			records = res.Data2
		} else {
			err = errors.New("Unknown data format")
			return
		}

		for _, record := range records {

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

			var ok bool
			_, ok = symbols[symbol]
			if !ok {
				symbols[symbol] = ts
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

			client.InsertRecordRow(
				&cassandra.RecordRow{
					RecordPrimaryKey: cassandra.RecordPrimaryKey{
						RecordPartitionKey: cassandra.RecordPartitionKey{
							Exchange: "TWSE",
							Symbol:   symbol,
							Period:   period,
						},
						Datetime: datetime.GetTime(ts, location),
					},
					Name:   name,
					Open:   open,
					High:   high,
					Low:    low,
					Close:  close,
					Volume: volume,
				},
			)
		}
	}

	logger.Debug(fmt.Sprintf("%d", len(symbols)))

	for symbol, ts := range symbols {

		logger.Debug(fmt.Sprintf("%s %d", symbol, ts))

		message := &protobuf.Analyzer{
			Exchange: "TWSE",
			Symbol:   symbol,
			Period:   "1d",
			Datetime: ts,
		}

		var bytes []byte

		bytes, err = proto.Marshal(message)
		if err != nil {
			return
		}

		err = producer.Produce(topic, 0, bytes)
		if err != nil {
			return
		}
	}

	return
}
