package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const (
	DATA_TYPE_GAUGE   = "GAUGE"
	DATA_TYPE_COUNTER = "COUNTER"
)

// 待发送到open-falcon agent的数据
type MetaData struct {
	Metric      string      `json:"metric"`      // key
	Endpoint    string      `json:"endpoint"`    // hostname
	Value       interface{} `json:"value"`       // number or string
	CounterType string      `json:"counterType"` // GAUGE  原值   COUNTER 差值(ps)
	Tags        string      `json:"tags"`        // port=3306,k=v
	Timestamp   int64       `json:"timestamp"`
	Step        int64       `json:"step"`
}

func (m *MetaData) String() string {
	s := fmt.Sprintf("MetaData Metric:%s Endpoint:%s Value:%v CounterType:%s Tags:%s Timestamp:%d Step:%d",
		m.Metric, m.Endpoint, m.Value, m.CounterType, m.Tags, m.Timestamp, m.Step)
	return s
}

func Hostname() string {
	host, _ := os.Hostname()
	return host
}

func SendData(data []*MetaData, falconClient string, timeout time.Duration) ([]byte, error) {

	js, err := json.Marshal(data)
	//	log.Print("MetricsData: ", js)
	if err != nil {
		return nil, err
	}

	http.DefaultClient.Timeout = timeout
	res, err := http.Post(falconClient, "Content-Type: application/json", bytes.NewBuffer(js))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}
