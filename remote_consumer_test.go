package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	testifyAssert "github.com/stretchr/testify/assert"
	"time"
)

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_local() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group",
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

	remoteConnectionParams.Brokers = s.RemoteWrapper.GetBrokerAddress()
	remoteConnectionParams.Conf = test_utils.CreateBasicConf()

	localConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	localConnectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	localProducer, _ := kafka_wrapper.NewProducer(localConnectionParams)

	// When
	_, _, _ = localProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage + "local"),
		Topic: topicParams.ErrorTopic,
	})

	testConsumer, _ := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_remote2() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group",
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

	remoteConnectionParams.Brokers = s.RemoteWrapper.GetBrokerAddress()
	remoteConnectionParams.Conf = test_utils.CreateBasicConf()

	localConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	localConnectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	remoteProducer, _ := kafka_wrapper.NewProducer(localConnectionParams)

	// When
	_, _, _ = remoteProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage + "remote"),
		Topic: topicParams.Topic,
	})

	testConsumer, _ := kafka_wrapper.NewRemoteConsumer(localConnectionParams, localConnectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_remote() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group",
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

	remoteConnectionParams.Brokers = s.RemoteWrapper.GetBrokerAddress()
	remoteConnectionParams.Conf = test_utils.CreateBasicConf()

	localConnectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	localConnectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	remoteProducer, _ := kafka_wrapper.NewProducer(remoteConnectionParams)

	// When

	_, _, _ = remoteProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})

	testConsumer, _ := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))

	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}
