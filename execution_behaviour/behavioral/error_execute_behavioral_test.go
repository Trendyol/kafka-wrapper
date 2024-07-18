package behavioral_test

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour/behavioral"
	"github.com/Trendyol/kafka-wrapper/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_error_executor_should_just_call_executor(t *testing.T) {
	// Given
	var (
		ctrl           = gomock.NewController(t)
		executorMock   = mocks.NewMockLogicOperator(ctrl)
		normalExecutor = behavioral.ErrorBehavioral(executorMock)
	)

	executorMock.EXPECT().Operate(gomock.Any(), gomock.Any()).Times(1)

	// When
	err := normalExecutor.Process(context.TODO(), &sarama.ConsumerMessage{Topic: "topic"})

	// Then
	assert.Nil(t, err)
}
