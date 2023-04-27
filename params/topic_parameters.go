package params

type TopicsParameters struct {
	Topic                 string
	ErrorTopic            string
	RetryTopic            string
	UsingSharedErrorTopic bool
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
		allTopics = append(allTopics, topic.getAllTopics()...)
	}
	return allTopics
}

func (p TopicsParameters) getAllTopics() []string {
	topics := make([]string, 0)
	if p.ErrorTopic != "" && !p.UsingSharedErrorTopic {
		topics = append(topics, p.ErrorTopic)
	}
	if p.RetryTopic != "" {
		topics = append(topics, p.RetryTopic)
	}
	topics = append(topics, p.Topic)
	return topics
}
