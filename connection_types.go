package kafka_wrapper

type ConnectionParameters struct {
	Brokers         string
	ClientID        string
	Version         string
	Topic           []string
	ErrorTopic      string
	RetryTopic      string
	ConsumerGroupID string
	KafkaUsername   string
	KafkaPassword   string
	ApplicationPort string
	FromBeginning   bool
}
