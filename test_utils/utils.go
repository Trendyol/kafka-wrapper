package test_utils

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/durmaze/gobank"
	"github.com/durmaze/gobank/responses"
	. "github.com/onsi/ginkgo"
	"github.com/parnurzeal/gorequest"
	"net/http"
	"sync"
	"time"
)

var WaitUntilApiUp = func(apiUrl string) {
	request := gorequest.New()
	WaitUntil(func() bool {
		response, _, _ := request.Get(apiUrl + "/health").End()

		if response != nil && response.StatusCode == 200 {
			return false
		}

		return true
	}, 30*time.Second, 500*time.Millisecond)
}

var BeforeAll = func(beforeAllFunc func()) {
	var once sync.Once

	BeforeEach(func() {
		once.Do(func() {
			beforeAllFunc()
		})
	})
}

var AfterAll = func(afterAllFunc func()) {
	var once sync.Once

	AfterEach(func() {
		once.Do(func() {
			afterAllFunc()
		})
	})
}


func WaitUntil(predicate func() bool, timeout time.Duration, interval time.Duration) bool {
	var totalWait time.Duration

	for predicate() == false && totalWait < timeout {
		time.Sleep(interval)
		totalWait = totalWait + interval
	}

	return totalWait < timeout
}

func Consume(kafkaConfig kafka_wrapper.ConnectionParameters) (string, string, []*sarama.RecordHeader) {
	v, _ := sarama.ParseKafkaVersion(kafkaConfig.Version)

	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumer([]string{kafkaConfig.Brokers}, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	partitions, _ := master.Partitions(kafkaConfig.Topic[0])
	partitionConsumer, err := master.ConsumePartition(kafkaConfig.Topic[0], partitions[0], sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	msg := <-partitionConsumer.Messages()
	return string(msg.Key), string(msg.Value), msg.Headers
}


// add match-all stub to return 404 for unmatched cases. Default behavior of Mountebank is to return 200 OK.
func NotFoundStub() gobank.StubElement {
	return gobank.Stub().
		Responses(responses.
			Is().
			StatusCode(http.StatusNotFound).
			Header("Content-Type", "application/json").
			Header("X-Mountebank-Catch-All", "Matched").
			Build()).
		Build()
}