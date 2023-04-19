package kafka_wrapper_test

import (
	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_it_should_set_full_data_retrieval_parameter_if_it_is_requested(t *testing.T) {
	// Given
	config := sarama.Config{}
	var manipulatedConfig *sarama.Config
	sut := kafka_wrapper.NewConfigurationManipulator(true)

	// When
	manipulatedConfig = sut.ManipulateMetadataRetrieval(&config)

	// Then
	assert.True(t, manipulatedConfig.Metadata.Full)
}

func Test_it_should_reset_full_data_retrieval_parameter_if_it_is_not_requested(t *testing.T) {
	// Given
	config := sarama.Config{}
	var manipulatedConfig *sarama.Config
	sut := kafka_wrapper.NewConfigurationManipulator(false)

	// When
	manipulatedConfig = sut.ManipulateMetadataRetrieval(&config)

	// Then
	assert.False(t, manipulatedConfig.Metadata.Full)
}
