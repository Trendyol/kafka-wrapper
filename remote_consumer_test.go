package kafka_wrapper_test

import (
	"github.com/IBM/sarama"
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
			ConsumerGroupID: "remote-consumer-group-local-test",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group-local-test",
		}

		topicParams = params.TopicsParameters{
			Topic:      "msg-local-main",
			RetryTopic: "msg-local-retry",
			ErrorTopic: "msg-local-error",
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

	localProducer, err := kafka_wrapper.NewProducer(localConnectionParams)
	if !assert.NoError(err) {
		return
	}

	// When
	_, _, err = localProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage + "local"),
		Topic: topicParams.ErrorTopic,
	})
	if !assert.NoError(err) {
		return
	}

	testConsumer, err := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	if !assert.NoError(err, "NewRemoteConsumer should not error") {
		return
	}
	defer testConsumer.Unsubscribe()

	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(expectedMessage+"local", receivedMessage)
}

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_remote2() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group-r2-test",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group-r2-test",
		}

		topicParams = params.TopicsParameters{
			Topic:      "msg-remote2-main",
			RetryTopic: "msg-remote2-retry",
			ErrorTopic: "msg-remote2-error",
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

	remoteProducer, err := kafka_wrapper.NewProducer(localConnectionParams)
	if !assert.NoError(err) {
		return
	}

	// When
	_, _, err = remoteProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage + "remote"),
		Topic: topicParams.Topic,
	})
	if !assert.NoError(err) {
		return
	}

	testConsumer, err := kafka_wrapper.NewRemoteConsumer(localConnectionParams, localConnectionParams)
	if !assert.NoError(err) {
		return
	}
	defer testConsumer.Unsubscribe()

	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(expectedMessage+"remote", receivedMessage)
}

func (s *testKafkaSuite) Test_consume_when_a_message_sent_to_remote() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		remoteConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "remote-consumer-group-r1-test",
		}

		localConnectionParams = params.ConnectionParameters{
			ConsumerGroupID: "local-consumer-group-r1-test",
		}

		topicParams = params.TopicsParameters{
			Topic:      "msg-remote1-main",
			RetryTopic: "msg-remote1-retry",
			ErrorTopic: "msg-remote1-error",
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

	remoteProducer, err := kafka_wrapper.NewProducer(remoteConnectionParams)
	if !assert.NoError(err) {
		return
	}

	// When

	_, _, err = remoteProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})
	if !assert.NoError(err) {
		return
	}

	testConsumer, err := kafka_wrapper.NewRemoteConsumer(remoteConnectionParams, localConnectionParams)
	if !assert.NoError(err) {
		return
	}
	defer testConsumer.Unsubscribe()

	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))

	receivedMessage = <-messageChn

	// Then
	assert.Equal(expectedMessage, receivedMessage)
}
