package kafka_wrapper

import "github.com/Shopify/sarama"

//go:generate mockgen -destination=./mocks/configuration_manipulator_mock.go -package=mocks -source=configuration_manipulator.go

type ConfigurationManipulation interface {
	ManipulateMetadataRetrieval(*sarama.Config) *sarama.Config
}

type configurationManipulator struct {
	fullDataRequired bool
}

func (c configurationManipulator) ManipulateMetadataRetrieval(config *sarama.Config) *sarama.Config {
	if c.fullDataRequired {
		config.Metadata.Full = true
	} else {
		config.Metadata.Full = false
	}
	return config
}

func NewConfigurationManipulator(fullDataRequired bool) ConfigurationManipulation {
	return &configurationManipulator{
		fullDataRequired: fullDataRequired,
	}
}
