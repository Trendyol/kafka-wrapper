package behavioral_test

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_retry_executor_should_call_executor_and_not_produce_message_when_executor_succeeds(t *testing.T) {
	// Given
	var (
		producerMock       = NewMockSyncProducer(false)
		errorTopic         = "ErrorTopicName"
		ctrl               = gomock.NewController(t)
		executorMock       = mocks.NewMockLogicOperator(ctrl)
		retryCount         = 3
		headerOperatorMock = mocks.NewMockheaderOperation(ctrl)
		retryTopicName     = "TheRetryTopic"
		retryExecutor      = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount, headerOperatorMock, nil)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	headerOperatorMock.EXPECT().ExtractHeader(gomock.Any()).Times(0)
	headerOperatorMock.EXPECT().AddIntoHeader(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: retryTopicName})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(0), producerMock.SendingCount)
}

func Test_retry_executor_should_call_executor_retry_limit_times_and_produce_message_with_header_manipulation_when_executor_fails(t *testing.T) {
	// Given
	var (
		producerMock       = NewMockSyncProducer(false)
		errorTopic         = "ErrorTopicName"
		ctrl               = gomock.NewController(t)
		executorMock       = mocks.NewMockLogicOperator(ctrl)
		retryCount         = 3
		retryTopicName     = "theRetryTopic"
		headerOperatorMock = mocks.NewMockheaderOperation(ctrl)
		retryExecutor      = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount, headerOperatorMock, nil)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(3).Return(errors.New("testingError"))
	headerOperatorMock.EXPECT().ExtractHeader(gomock.Any()).Return(nil)
	headerOperatorMock.EXPECT().AddIntoHeader(nil, behavioral.RetryTopicKey, retryTopicName).Return(nil)
	headerOperatorMock.EXPECT().AddIntoHeader(nil, behavioral.ErrorKey, "testingError").Return(nil)

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: retryTopicName})

	// Then
	assert.Nil(t, err)
	assert.Equal(t, int32(1), producerMock.SendingCount)
	assert.Equal(t, errorTopic, producerMock.Message.Topic)
}

func Test_retry_executor_should_fail_when_producing_operation_fails(t *testing.T) {
	// Given
	var (
		producerMock       = NewMockSyncProducer(true)
		errorTopic         = "ErrorTopicName"
		ctrl               = gomock.NewController(t)
		executorMock       = mocks.NewMockLogicOperator(ctrl)
		retryCount         = 3
		headerOperatorMock = mocks.NewMockheaderOperation(ctrl)
		retryExecutor      = behavioral.RetryBehavioral(producerMock, errorTopic, executorMock, retryCount, headerOperatorMock, nil)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(3).Return(errors.New("testingError"))
	headerOperatorMock.EXPECT().ExtractHeader(gomock.Any()).Return(nil)
	headerOperatorMock.EXPECT().AddIntoHeader(gomock.Any(), gomock.Any(), gomock.Any()).Times(2).Return(nil)

	// When
	err := retryExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.NotNil(t, err)
}
