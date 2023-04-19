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

func (s *testKafkaSuite) Test_producer_should_manipulate_the_given_metadata_configuration() {
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

	kafka_wrapper.NewProducer(manipulatorMock, connectionParams)
}

func (s *testKafkaSuite) Test_produce_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		message          = "test"
		connectionParams = kafka_wrapper.ConnectionParameters{ConsumerGroupID: "some-id"}
		topic            = "some-topic"

		receivedPayload string
		err             error
		producer        sarama.SyncProducer
		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()
	time.Sleep(5 * time.Second)
	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(connectionParams.Conf).Times(1).Return(connectionParams.Conf)

	producer, err = kafka_wrapper.NewProducer(manipulatorMock, connectionParams)

	// When
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(message),
		Topic: topic,
	})
	time.Sleep(5 * time.Second)
	_, receivedPayload, _ = test_utils.Consume(connectionParams, topic)

	// Then
	assert.Nil(err)
	assert.Equal(receivedPayload, message)
}

func (s *testKafkaSuite) Test_not_produce_when_broker_is_not_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		wrongConf = kafka_wrapper.ConnectionParameters{ConsumerGroupID: "some-id"}

		expectedError   error
		ctrl            = gomock.NewController(s.T())
		manipulatorMock = mocks.NewMockConfigurationManipulation(ctrl)
	)

	wrongConf.Brokers = "localhost:9093"
	wrongConf.Conf = test_utils.CreateBasicConf()

	manipulatorMock.EXPECT().ManipulateMetadataRetrieval(wrongConf.Conf).Times(1).Return(wrongConf.Conf)

	// When
	_, expectedError = kafka_wrapper.NewProducer(manipulatorMock, wrongConf)

	// Then
	assert.NotNil(expectedError)
}
