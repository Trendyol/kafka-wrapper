package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/mocks"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	"github.com/golang/mock/gomock"
	testifyAssert "github.com/stretchr/testify/assert"
	"time"
)

func (s *testKafkaSuite) Test_consumer_should_manipulate_the_given_metadata_configuration() {
	// Given
	var (
		connectionParams = kafka_wrapper.ConnectionParameters{
			ConsumerGroupID: "consumer-group",
		}
		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()

	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(connectionParams.Conf).Times(1).Return(connectionParams.Conf)

	kafka_wrapper.NewConsumer(manipulatorMock, connectionParams)
}

func (s *testKafkaSuite) Test_consume_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		connectionParams = kafka_wrapper.ConnectionParameters{
			ConsumerGroupID: "consumer-group",
		}
		topicParams = kafka_wrapper.TopicsParameters{
			Topic:      "test-topic",
			RetryTopic: "test-topic_retry",
			ErrorTopic: "test-topic_error",
		}
		expectedMessage = "test"
		messageChn      = make(chan string, 1)
		receivedMessage string
		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()
	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(connectionParams.Conf).Times(2).Return(connectionParams.Conf)

	time.Sleep(5 * time.Second)

	testProducer, _ := kafka_wrapper.NewProducer(manipulatorMock, connectionParams)

	// When
	_, _, _ = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage),
		Topic: topicParams.Topic,
	})
	testConsumer, _ := kafka_wrapper.NewConsumer(manipulatorMock, connectionParams)
	testConsumer.SubscribeToTopic(topicParams, test_utils.NewEventHandler(messageChn))
	receivedMessage = <-messageChn

	// Then
	assert.Equal(receivedMessage, expectedMessage)
}

func (s *testKafkaSuite) Test_consume_multiple_topic_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		connectionParams = kafka_wrapper.ConnectionParameters{
			ConsumerGroupID: "consumer-group",
		}
		testTopic1Params = kafka_wrapper.TopicsParameters{
			Topic:      "test-topic1",
			RetryTopic: "test-topic1_retry",
			ErrorTopic: "test-topic1_error",
		}
		testTopic2Params = kafka_wrapper.TopicsParameters{
			Topic:      "test-topic2",
			RetryTopic: "test-topic2_retry",
			ErrorTopic: "test-topic2_error",
		}

		expectedMessage1 = "test1"
		expectedMessage2 = "test2"
		receivedMessage1 string
		receivedMessage2 string

		messageChn = make(chan string, 1)

		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()

	topics := kafka_wrapper.FromTopics(testTopic1Params, testTopic2Params)

	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(connectionParams.Conf).Times(2).Return(connectionParams.Conf)

	time.Sleep(5 * time.Second)
	testProducer, _ := kafka_wrapper.NewProducer(manipulatorMock, connectionParams)

	// When && Then
	testConsumer, _ := kafka_wrapper.NewConsumer(manipulatorMock, connectionParams)
	testConsumer.Subscribe(topics, test_utils.NewEventHandler(messageChn))

	_, _, _ = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage1),
		Topic: testTopic1Params.Topic,
	})
	receivedMessage1 = <-messageChn
	assert.Equal(receivedMessage1, expectedMessage1)

	time.Sleep(5 * time.Second)

	_, _, _ = testProducer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(expectedMessage2),
		Topic: testTopic2Params.Topic,
	})
	receivedMessage2 = <-messageChn
	assert.Equal(receivedMessage2, expectedMessage2)
}

func (s *testKafkaSuite) Test_not_consume_when_broker_is_not_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		wrongConf = kafka_wrapper.ConnectionParameters{
			Conf:    sarama.NewConfig(),
			Brokers: "localhost:9093",
		}
		expectedError error

		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)
	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(wrongConf.Conf).Times(1).Return(wrongConf.Conf)
	// When
	_, expectedError = kafka_wrapper.NewConsumer(manipulatorMock, wrongConf)

	// Then
	assert.NotNil(expectedError)
}
