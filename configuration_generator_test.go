package kafka_wrapper_test

import (
	"github.com/IBM/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_it_should_reset_full_data_retrieval_parameter(t *testing.T) {
	// When
	config := kafka_wrapper.GenerateKafkaConfiguration("2.2.0", "clientId", false)

	// Then
	assert.False(t, config.Metadata.Full)
}

func Test_it_should_generate_configuration_to_wait_for_all_brokers_when_publishing(t *testing.T) {
	// When
	config := kafka_wrapper.GenerateKafkaConfiguration("2.2.0", "clientId", true)

	// Then
	assert.Equal(t, sarama.WaitForAll, config.Producer.RequiredAcks)
}

func Test_it_should_generate_configuration_to_wait_for_local_broker_when_publishing(t *testing.T) {
	// When
	config := kafka_wrapper.GenerateKafkaConfiguration("2.2.0", "clientId", false)

	// Then
	assert.Equal(t, sarama.WaitForLocal, config.Producer.RequiredAcks)
}
