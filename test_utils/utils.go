package test_utils

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	. "github.com/onsi/ginkgo"
	"github.com/parnurzeal/gorequest"
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

	master, err := sarama.NewConsumer([]string{kafkaConfig.Brokers}, kafkaConfig.Conf)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	partitions, _ := master.Partitions(kafkaConfig.Topics[0])
	partitionConsumer, err := master.ConsumePartition(kafkaConfig.Topics[0], partitions[0], sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	msg := <-partitionConsumer.Messages()
	return string(msg.Key), string(msg.Value), msg.Headers
}
