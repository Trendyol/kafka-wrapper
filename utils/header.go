package utils

import "github.com/Shopify/sarama"

//go:generate mockgen -destination=../mocks/header_mock.go -package=mocks -source=header.go

type HeaderOperation interface {
	AddIntoHeader(headers []sarama.RecordHeader, key, value string) []sarama.RecordHeader
	ExtractHeader(message *sarama.ConsumerMessage) []sarama.RecordHeader
}

type headerOperator struct {
}

func NewHeaderOperator() HeaderOperation {
	return &headerOperator{}
}

func (h *headerOperator) AddIntoHeader(headers []sarama.RecordHeader, key, value string) []sarama.RecordHeader {
	for i, header := range headers {
		if string(header.Key) == key {
			headers[i].Value = []byte(value)
			return headers
		}
	}
	newHeader := sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	}
	return append(headers, newHeader)
}

func (h *headerOperator) ExtractHeader(message *sarama.ConsumerMessage) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)

	for i := 0; i < len(message.Headers); i++ {
		headers = append(headers, *message.Headers[i])
	}

	return headers
}
