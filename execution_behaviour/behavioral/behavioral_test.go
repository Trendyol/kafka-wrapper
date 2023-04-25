package behavioral_test

import (
	"errors"
	"github.com/Shopify/sarama"
)

type mockSyncProducer struct {
	SendingCount int32
	Message      *sarama.ProducerMessage
	shouldFail   bool
}

func NewMockSyncProducer(shouldFail bool) *mockSyncProducer {
	return &mockSyncProducer{SendingCount: 0, Message: nil, shouldFail: shouldFail}
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	m.SendingCount++
	m.Message = msg
	err = nil
	if m.shouldFail {
		err = errors.New("testingError")
	}
	return 0, 0, err
}

func (m *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	m.SendingCount++
	return nil
}

func (m *mockSyncProducer) Close() error                            { return nil }
func (m *mockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag { return 0 }
func (m *mockSyncProducer) IsTransactional() bool                   { return false }
func (m *mockSyncProducer) BeginTxn() error                         { return nil }
func (m *mockSyncProducer) CommitTxn() error                        { return nil }
func (m *mockSyncProducer) AbortTxn() error                         { return nil }
func (m *mockSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}
func (m *mockSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}
