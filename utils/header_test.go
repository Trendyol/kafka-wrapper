package utils_test

import (
	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_header_operator_should_extract_existing_headers(t *testing.T) {
	// Given
	headers := make([]*sarama.RecordHeader, 0)
	headers = append(headers, &sarama.RecordHeader{
		Key:   []byte("key_1"),
		Value: []byte("val_1"),
	})
	headers = append(headers, &sarama.RecordHeader{
		Key:   []byte("key_2"),
		Value: []byte("val_2"),
	})

	message := sarama.ConsumerMessage{
		Headers: headers,
	}
	sut := utils.NewHeaderOperator()

	// When
	extracted := sut.ExtractHeader(&message)

	// Then
	assert.Equal(t, 2, len(extracted))
	assert.Equal(t, "key_1", string(extracted[0].Key))
	assert.Equal(t, "key_2", string(extracted[1].Key))
	assert.Equal(t, "val_1", string(extracted[0].Value))
	assert.Equal(t, "val_2", string(extracted[1].Value))
}

func Test_header_operator_should_add_given_item_to_the_header(t *testing.T) {
	// Given
	headers := make([]sarama.RecordHeader, 0)
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("key_1"),
		Value: []byte("val_1"),
	})
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("key_2"),
		Value: []byte("val_2"),
	})

	sut := utils.NewHeaderOperator()

	// When
	extendedHeader := sut.AddIntoHeader(headers, "inserter_key", "inserted_val")

	// Then
	assert.Equal(t, 3, len(extendedHeader))
	assert.Equal(t, "inserter_key", string(extendedHeader[2].Key))
	assert.Equal(t, "inserted_val", string(extendedHeader[2].Value))
}
