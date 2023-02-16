package kafka_wrapper

import "github.com/Shopify/sarama"

type ConnectionParameters struct {
	Conf            *sarama.Config
	Brokers         string
	ConsumerGroupID string
}

type TopicsParameters struct {
	Topic      string
	ErrorTopic string
	RetryTopic string
}

func (p TopicsParameters) GetAllTopics() []string {
	topics := make([]string, 0)
	if p.ErrorTopic != "" {
		topics = append(topics, p.ErrorTopic)
	}
	if p.RetryTopic != "" {
		topics = append(topics, p.RetryTopic)
	}
	topics = append(topics, p.Topic)
	return topics
}

func FromTopics(topics ...TopicsParameters) []TopicsParameters {
	allTopics := make([]TopicsParameters, 0)
	for _, topic := range topics {
		allTopics = append(allTopics, topic)
	}
	return allTopics
}

func JoinAllTopics(topics []TopicsParameters) []string {
	allTopics := make([]string, 0)
	for _, topic := range topics {
		allTopics = append(allTopics, topic.GetAllTopics()...)
	}
	return allTopics
}
