package behavioral

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
)

const ErrorKey = "ErrorMessage"
const RetryKey = "RetryCount"

type retryBehaviour struct {
	producer   sarama.SyncProducer
	executor   LogicOperator
	retryCount int
	errorTopic string
}

func RetryBehavioral(producer sarama.SyncProducer, errorTopic string, executor LogicOperator, retryCount int) BehaviourExecutor {
	return &retryBehaviour{
		producer:   producer,
		executor:   executor,
		retryCount: retryCount,
		errorTopic: errorTopic,
	}
}

func (k *retryBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) (err error) {

	for i := 0; i < k.retryCount; i++ {
		if i == 0 {
			latestExecutableTime := time.Now().Add(time.Duration(-5) * time.Second)
			if latestExecutableTime.Before(message.Timestamp) {
				sleepTime := message.Timestamp.Sub(latestExecutableTime)
				kafka_wrapper.Logger.Printf("System will sleep for %+v\n", sleepTime)
				fmt.Printf("System will sleep for %+v\n", sleepTime)
				time.Sleep(sleepTime)
			} else {
				fmt.Printf("No need to sleep for message time %+v\n", message.Timestamp)
			}
		} else {
			time.Sleep(250 * time.Millisecond)
		}

		err = k.executor.Operate(ctx, message)
		if err == nil {
			kafka_wrapper.Logger.Printf("Message is executed successfully, message: %+v\n", string(message.Value))
			break
		}
	}

	if err != nil {
		kafka_wrapper.Logger.Printf("Message is not executed successfully: %+v so is routing to error topic: %+v, message: %+v\n", message.Topic, k.errorTopic, message)
		err = k.sendToErrorTopic(message, k.errorTopic, err.Error())
		if err != nil {
			kafka_wrapper.Logger.Printf("Message is not published to error topic: %+v\n", k.errorTopic)
			return err
		}
	}

	return nil
}

func (k *retryBehaviour) sendToErrorTopic(message *sarama.ConsumerMessage, errorTopic string, errorMessage string) error {
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Headers: prepareHeadersWithErrorMessage(message, errorMessage),
		Key:     sarama.StringEncoder(message.Key),
		Topic:   errorTopic,
		Value:   sarama.StringEncoder(message.Value),
	})
	return err
}

func prepareHeadersWithErrorMessage(message *sarama.ConsumerMessage, errorMessage string) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)

	for i := 0; i < len(message.Headers); i++ {
		if string(message.Headers[i].Key) == ErrorKey {
			continue
		}
		headers = append(headers, *message.Headers[i])
	}

	headers = append(headers, sarama.RecordHeader{
		Key:   []byte(ErrorKey),
		Value: []byte(errorMessage),
	})

	return headers
}
