package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/test_utils"
	testifyAssert "github.com/stretchr/testify/assert"
	"time"
)

func (s *testKafkaSuite) Test_produce_when_broker_is_reachable() {
	// Given
	var (
		assert = testifyAssert.New(s.T())

		message          = "test"
		connectionParams = params.ConnectionParameters{ConsumerGroupID: "some-id"}
		topic            = "some-topic"

		receivedPayload string
		err             error
		producer        sarama.SyncProducer
	)

	connectionParams.Brokers = s.Wrapper.GetBrokerAddress()
	connectionParams.Conf = test_utils.CreateBasicConf()
	time.Sleep(5 * time.Second)

	producer, err = kafka_wrapper.NewProducer(connectionParams)

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

		wrongConf = params.ConnectionParameters{ConsumerGroupID: "some-id"}

		expectedError error
	)

	wrongConf.Brokers = "localhost:9093"
	wrongConf.Conf = test_utils.CreateBasicConf()

	// When
	_, expectedError = kafka_wrapper.NewProducer(wrongConf)

	// Then
	assert.NotNil(expectedError)
}
