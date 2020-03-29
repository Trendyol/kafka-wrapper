package kafka_wrapper_test

import (
	"github.com/Trendyol/kafka-wrapper/internal/integrationcontainers/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestProducer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kafka Wrapper Suite")
}

var (
	KafkaContainer *kafka.Container
)

var _ = BeforeSuite(func() {
	KafkaContainer = kafka.NewContainer("johnnypark/kafka-zookeeper")
	KafkaContainer.Run()
})

var _ = AfterSuite(func() {
	KafkaContainer.ForceRemoveAndPrune()
})
