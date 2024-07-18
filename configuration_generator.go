package kafka_wrapper

import (
	"github.com/IBM/sarama"
	"time"
)

func GenerateKafkaConfiguration(version, clientId string, shouldWaitAckFromAll bool) *sarama.Config {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		panic(err)
	}
	//consumer
	config := sarama.NewConfig()
	config.Version = v
	config.Consumer.Return.Errors = true
	config.ClientID = clientId

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.Metadata.Full = false

	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 3 * time.Second
	config.Consumer.Fetch.Default = 2048 * 1024
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	config.Consumer.Group.Rebalance.Timeout = 3 * time.Minute

	//producer
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 10 * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 3 * time.Minute
	config.Net.ReadTimeout = 3 * time.Minute
	config.Net.DialTimeout = 3 * time.Minute
	config.Net.WriteTimeout = 3 * time.Minute
	config.Producer.MaxMessageBytes = 2000000

	if shouldWaitAckFromAll {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	return config
}
