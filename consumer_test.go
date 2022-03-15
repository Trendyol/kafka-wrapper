package kafka_wrapper_test

import (
	"time"

	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("When consuming a message", func() {
	Context("and the broker is reachable", func() {
		var (
			conf = kafka_wrapper.ConnectionParameters{
				ConsumerGroupID: "consumerGroup",
				Topics:          []string{"testtopic"},
				RetryTopic:      "testtopic_RETRY",
				ErrorTopic:      "testtopic_ERROR",
			}
			expectedMessage   = "test"
			messageChn        = make(chan string, 1)
			receivedMessage   string
			subscriptionError error
		)

		test_utils.BeforeAll(func() {
			conf.Brokers = KafkaContainer.Address()
			conf.Conf = configuration("2.2.0")

			time.Sleep(5 * time.Second)
			testProducer, err := kafka_wrapper.NewProducer(conf)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = testProducer.SendMessage(&sarama.ProducerMessage{
				Value: sarama.StringEncoder(expectedMessage),
				Topic: conf.Topics[0],
			})
			Expect(err).NotTo(HaveOccurred())
			testConsumer, err := kafka_wrapper.NewConsumer(conf)
			Expect(err).NotTo(HaveOccurred())
			testConsumer.Subscribe(newEventHandler(messageChn))
			receivedMessage = <-messageChn
		})

		It("should not produce error", func() {
			Expect(subscriptionError).NotTo(HaveOccurred())
		})

		It("should consume the expected expectedMessage", func() {
			Expect(receivedMessage).Should(Equal(expectedMessage))
		})
	})

	Context("and the broker is unreachable", func() {
		var (
			wrongConf = kafka_wrapper.ConnectionParameters{
				Conf:       sarama.NewConfig(),
				Brokers:    "localhost:9093",
				Topics:     []string{"createClaim2"},
				RetryTopic: "createClaim2_RETRY",
				ErrorTopic: "createClaim2_ERROR",
			}
			expectedError error
		)

		test_utils.BeforeAll(func() {
			_, expectedError = kafka_wrapper.NewConsumer(wrongConf)
		})

		It("should produce an error", func() {
			Expect(expectedError).To(HaveOccurred())
		})
	})
})

func newEventHandler(message chan string) kafka_wrapper.EventHandler {
	return &testEventHandler{
		message:              message,
		subscriptionStatusCh: make(chan bool),
	}
}

type testEventHandler struct {
	message              chan string
	subscriptionStatusCh chan bool
}

func (ge *testEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (ge *testEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		ge.message <- string(message.Value)
	}

	return nil
}
