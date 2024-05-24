package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	testifyAssert "github.com/stretchr/testify/assert"
	"time"
)

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_remote() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		topicParams = params.TopicsParameters{
			Topic:      "test-topic",
			RetryTopic: "test-topic_retry",
			ErrorTopic: "test-topic_error",
		}
		expectedMessage = "test"
		messageChn      = make(chan string, 1)
		receivedMessage string
	)

	remoteConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	remoteConnectionParams.Conf = test_utils.CreateBasicConf()

	localConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	localConnectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	testProducer, _ := kafka_wrapper.NewProducer(remoteConnectionParams)

	// When
	_, _, _ = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})
	testConsumer, _ := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandlerWithError(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_local() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		topicParams = params.TopicsParameters{
			Topic:      "test-topic",
			RetryTopic: "test-topic_retry",
			ErrorTopic: "test-topic_error",
		}
		expectedMessage = "test"
		messageChn      = make(chan string, 1)
		receivedMessage string
	)

	remoteConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	remoteConnectionParams.Conf = test_utils.CreateBasicConf()

	localConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	localConnectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	testProducer, _ := kafka_wrapper.NewProducer(localConnectionParams)

	// When
	_, _, _ = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})
	testConsumer, _ := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandlerWithError(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}
