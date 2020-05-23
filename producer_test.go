package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("When publishing a message", func() {

	Context("and the broker is reachable", func() {
		var (
			message = "test"
			conf    = kafka_wrapper.ConnectionParameters{
				ConsumerGroupID: "some-id",
				Topics:          []string{"createClaim"},
			}
			receivedPayload string
			err             error
			producer        sarama.SyncProducer
		)

		test_utils.BeforeAll(func() {
			conf.Brokers = KafkaContainer.Address()
			conf.Conf = configuration("2.2.0")
			time.Sleep(5 * time.Second)

			producer, err = kafka_wrapper.NewProducer(conf)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Value: sarama.StringEncoder(message),
				Topic: conf.Topics[0],
			})
			time.Sleep(5 * time.Second)
			_, receivedPayload, _ = test_utils.Consume(conf)
		})

		It("should not produce an error", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("should send the message with expected payload", func() {
			Expect(receivedPayload).Should(Equal(message))
		})
	})

	Context("and the broker is unreachable", func() {
		var (
			wrongConf = kafka_wrapper.ConnectionParameters{
				Conf:    sarama.NewConfig(),
				Brokers: "localhost:9093",
				Topics:  []string{"createClaim"},
			}
			expectedError error
		)

		test_utils.BeforeAll(func() {
			_, expectedError = kafka_wrapper.NewProducer(wrongConf)
		})

		It("should produce an error", func() {
			Expect(expectedError).To(HaveOccurred())
		})
	})

})

func configuration(version string) *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	v, err := sarama.ParseKafkaVersion("2.2.0")
	Expect(err).NotTo(HaveOccurred())
	kafkaConfig.Version = v
	kafkaConfig.ClientID = "oms-event-generator"
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Consumer.Return.Errors = true

	return kafkaConfig
}
