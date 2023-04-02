package execution_behaviour_test

import (
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/execution_behaviour"
	"github.com/Trendyol/kafka-wrapper/mocks"
	"github.com/golang/mock/gomock"
	"testing"
)

func Test_it_should_select_normal_behaviour_for_simple_topic(t *testing.T) {
	// Given
	var (
		ctrl           = gomock.NewController(t)
		normalBehavior = mocks.NewMockBehaviourExecutor(ctrl)
		retryBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		errorBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		selector       = execution_behaviour.NewBehaviourSelector(normalBehavior, retryBehavior, errorBehavior)
		claim          = NewMockConsumerGroupClaim("an_ordinary_topic.of_any_team")
	)
	normalBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(1)
	retryBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)
	errorBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)

	// When
	behavior := selector.GetBehavior(claim)
	behavior.Process(nil, nil)
}

func Test_it_should_select_retry_behaviour_for_retry_topic(t *testing.T) {
	// Given
	var (
		ctrl           = gomock.NewController(t)
		normalBehavior = mocks.NewMockBehaviourExecutor(ctrl)
		retryBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		errorBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		selector       = execution_behaviour.NewBehaviourSelector(normalBehavior, retryBehavior, errorBehavior)
		claim          = NewMockConsumerGroupClaim("an_ordinary_topic.of_any_team_retry")
	)
	normalBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)
	retryBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(1)
	errorBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)

	// When
	behavior := selector.GetBehavior(claim)
	behavior.Process(nil, nil)
}

func Test_it_should_select_error_behaviour_for_error_topic(t *testing.T) {
	// Given
	var (
		ctrl           = gomock.NewController(t)
		normalBehavior = mocks.NewMockBehaviourExecutor(ctrl)
		retryBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		errorBehavior  = mocks.NewMockBehaviourExecutor(ctrl)
		selector       = execution_behaviour.NewBehaviourSelector(normalBehavior, retryBehavior, errorBehavior)
		claim          = NewMockConsumerGroupClaim("an_ordinary_topic.of_any_team_error")
	)
	normalBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)
	retryBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(0)
	errorBehavior.EXPECT().Process(gomock.Any(), gomock.Any()).Times(1)

	// When
	behavior := selector.GetBehavior(claim)
	behavior.Process(nil, nil)
}

// MockClaim
type mockConsumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
}

func NewMockConsumerGroupClaim(topic string) *mockConsumerGroupClaim {
	return &mockConsumerGroupClaim{
		topic: topic,
	}
}

func (c *mockConsumerGroupClaim) Topic() string                            { return c.topic }
func (c *mockConsumerGroupClaim) Partition() int32                         { return 0 }
func (c *mockConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (c *mockConsumerGroupClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return nil }
