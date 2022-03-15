package kafka_wrapper_test

import (
	"testing"

	"github.com/Trendyol/kafka-wrapper/internal/integrationcontainers/kafka"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
