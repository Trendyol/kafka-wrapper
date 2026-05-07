package kafka_wrapper_test

import (
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	testifyAssert "github.com/stretchr/testify/assert"
	"time"
)

func (s *testKafkaSuite) Test_consume_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		connectionParams = params.ConnectionParameters{
			ConsumerGroupID: "consumer-group-broker-reachable",
		}
		topicParams = params.TopicsParameters{
			Topic:      "brk-reachable-main",
			RetryTopic: "brk-reachable-retry",
			ErrorTopic: "brk-reachable-error",
		}
		expectedMessage = "test"
		messageChn      = make(chan string, 1)
		receivedMessage string
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	testProducer, err := kafka_wrapper.NewProducer(connectionParams)
	if !assert.NoError(err) {
		return
	}

	// When
	_, _, err = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})
	if !assert.NoError(err) {
		return
	}

	testConsumer, err := kafka_wrapper.NewConsumer(connectionParams)
	if !assert.NoError(err) {
		return
	}
	defer testConsumer.Unsubscribe()

	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(expectedMessage, receivedMessage)
}

func (s *testKafkaSuite) Test_consume_multiple_topic_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		connectionParams = params.ConnectionParameters{
			ConsumerGroupID: "consumer-group-multi-topic",
		}
		testTopic1Params = params.TopicsParameters{
			Topic:      "multi-topic-one-main",
			RetryTopic: "multi-topic-one-retry",
			ErrorTopic: "multi-topic-one-error",
		}
		testTopic2Params = params.TopicsParameters{
			Topic:      "multi-topic-two-main",
			RetryTopic: "multi-topic-two-retry",
			ErrorTopic: "multi-topic-two-error",
		}

		expectedMessage1 = "test1"
		expectedMessage2 = "test2"
		receivedMessage1 string
		receivedMessage2 string

		messageChn = make(chan string, 1)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()

	topics := params.FromTopics(testTopic1Params, testTopic2Params)

	time.Sleep(5 * time.Second)
	testProducer, err := kafka_wrapper.NewProducer(connectionParams)
	if !assert.NoError(err) {
		return
	}

	// When && Then
	testConsumer, err := kafka_wrapper.NewConsumer(connectionParams)
	if !assert.NoError(err) {
		return
	}
	defer testConsumer.Unsubscribe()

	testConsumer.Subscribe(topics, test_utils.NewEventHandler(messageChn))

	_, _, err = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage1),
		Topic: testTopic1Params.Topic,
	})
	if !assert.NoError(err) {
		return
	}
	receivedMessage1 = <-messageChn
	assert.Equal(expectedMessage1, receivedMessage1)

	time.Sleep(5 * time.Second)

	_, _, err = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage2),
		Topic: testTopic2Params.Topic,
	})
	if !assert.NoError(err) {
		return
	}
	receivedMessage2 = <-messageChn
	assert.Equal(expectedMessage2, receivedMessage2)
}

func (s *testKafkaSuite) Test_not_consume_when_broker_is_not_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		wrongConf = params.ConnectionParameters{
			Conf:    sarama.NewConfig(),
			Brokers: "127.0.0.1:65531",
		}
		expectedError error
	)

	// When
	_, expectedError = kafka_wrapper.NewConsumer(wrongConf)

	// Then
	assert.NotNil(expectedError)
}

func (s *testKafkaSuite) Test_stop_consume_after_unsubscription() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		connectionParams = params.ConnectionParameters{
			ConsumerGroupID: "consumer-group-unsubscribe",
		}
		topicParams = params.TopicsParameters{
			Topic:      "unsub-main",
			RetryTopic: "unsub-retry",
			ErrorTopic: "unsub-error",
		}
		messageChn = make(chan string, 1)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()

	time.Sleep(5 * time.Second)

	// When
	testConsumer, err := kafka_wrapper.NewConsumer(connectionParams)
	assert.NoError(err, "NewConsumer should not error")
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))

	testConsumer.Unsubscribe()

	testProducer, err := kafka_wrapper.NewProducer(connectionParams)
	assert.NoError(err, "NewProducer should not error")
	_, _, err = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder("should-not-be-received"),
		Topic: topicParams.Topic,
	})
	assert.NoError(err, "SendMessage should not error")

	// Then
	select {
	case msg := <-messageChn:
		assert.FailNow("Received message after unsubscription: " + msg)
	case <-time.After(3 * time.Second):
		// No message received: Test passes.
	}
}
