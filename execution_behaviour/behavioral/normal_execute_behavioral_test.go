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

func Test_normal_executor_should_call_executor_and_not_produce_message_when_executor_succeeds(t *testing.T) {
	// Given
	var (
		producerMock   = NewMockSyncProducer(false)
		retryTopic     = "RetryTopicName"
		ctrl           = gomock.NewController(t)
		executorMock   = mocks.NewMockLogicOperator(ctrl)
		normalExecutor = behavioral.NormalBehavioral(producerMock, retryTopic, executorMock)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1).Return(nil)

	// When
	err := normalExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(0), producerMock.SendingCount)
}

func Test_normal_executor_should_call_executor_and_produce_message_when_executor_fails(t *testing.T) {
	// Given
	var (
		producerMock   = NewMockSyncProducer(false)
		retryTopic     = "RetryTopicName"
		ctrl           = gomock.NewController(t)
		executorMock   = mocks.NewMockLogicOperator(ctrl)
		normalExecutor = behavioral.NormalBehavioral(producerMock, retryTopic, executorMock)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1).Return(errors.New("testingError"))

	// When
	err := normalExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(1), producerMock.SendingCount)
	assert.Equal(t, retryTopic, producerMock.Message.Topic)
}

func Test_normal_executor_should_fail_when_message_producing_operations_fails(t *testing.T) {
	// Given
	var (
		producerMock   = NewMockSyncProducer(true)
		retryTopic     = "RetryTopicName"
		ctrl           = gomock.NewController(t)
		executorMock   = mocks.NewMockLogicOperator(ctrl)
		normalExecutor = behavioral.NormalBehavioral(producerMock, retryTopic, executorMock)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1).Return(errors.New("testingError"))

	// When
	err := normalExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.NotNil(t, err)
}
