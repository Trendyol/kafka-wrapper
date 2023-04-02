package behavioral_test

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_retry_executor_should_call_executor_and_not_produce_message_when_executor_succeeds(t *testing.T) {
	// Given
	var (
		producerMock  = NewMockSyncProducer(false)
		errorTopic    = "ErrorTopicName"
		ctrl          = gomock.NewController(t)
		executorMock  = mocks.NewMockLogicOperator(ctrl)
		retryCount    = 3
		retryExecutor = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(0), producerMock.SendingCount)
}

func Test_retry_executor_should_call_executor_retry_limit_times_and_produce_message_when_executor_fails(t *testing.T) {
	// Given
	var (
		producerMock  = NewMockSyncProducer(false)
		errorTopic    = "ErrorTopicName"
		ctrl          = gomock.NewController(t)
		executorMock  = mocks.NewMockLogicOperator(ctrl)
		retryCount    = 3
		retryExecutor = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(3).Return(errors.New("testingError"))

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(1), producerMock.SendingCount)
	assert.Equal(t, errorTopic, producerMock.Message.Topic)
}

func Test_retry_executor_should_fail_when_producing_operation_fails(t *testing.T) {
	// Given
	var (
		producerMock  = NewMockSyncProducer(true)
		errorTopic    = "ErrorTopicName"
		ctrl          = gomock.NewController(t)
		executorMock  = mocks.NewMockLogicOperator(ctrl)
		retryCount    = 3
		retryExecutor = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(3).Return(errors.New("testingError"))

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.NotNil(t, err)
}
