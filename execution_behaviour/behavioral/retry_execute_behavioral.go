package behavioral

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper/utils"
	"time"
)

const ErrorKey = "ErrorMessage"
const RetryTopicKey = "RetryTopic"
const RetryErrorKey = "RetryErrorMessage"

type retryBehaviour struct {
	producer       sarama.SyncProducer
	executor       LogicOperator
	retryCount     int
	errorTopic     string
	headerOperator utils.HeaderOperation
}

func RetryBehavioral(producer sarama.SyncProducer, errorTopic string, executor LogicOperator, retryCount int, headerOperator utils.HeaderOperation) BehaviourExecutor {
	return &retryBehaviour{
		producer:       producer,
		executor:       executor,
		retryCount:     retryCount,
		errorTopic:     errorTopic,
		headerOperator: headerOperator,
	}
}

func (k *retryBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) (err error) {

	for i := 0; i < k.retryCount; i++ {
		if i == 0 {
			latestExecutableTime := time.Now().Add(time.Duration(-5) * time.Second)
			if latestExecutableTime.Before(message.Timestamp) {
				sleepTime := message.Timestamp.Sub(latestExecutableTime)
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
			fmt.Printf("Message is executed successfully, message: %+v\n", string(message.Value))
			break
		}
	}

	if err != nil {
		fmt.Printf("Message is not executed successfully: %+v so routing it to the error topic: %+v \n", message.Topic, k.errorTopic)
		err = k.sendToErrorTopic(message, err.Error())
		if err != nil {
			fmt.Printf("Message is not published to the error topic: %+v\n", k.errorTopic)
			return err
		}
	}

	return nil
}

func (k *retryBehaviour) sendToErrorTopic(message *sarama.ConsumerMessage, errorMessage string) error {

	headers := k.updateCurrentHeaders(message, errorMessage)

	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Headers: headers,
		Key:     sarama.StringEncoder(message.Key),
		Topic:   k.errorTopic,
		Value:   sarama.StringEncoder(message.Value),
	})
	return err
}

func (k *retryBehaviour) updateCurrentHeaders(message *sarama.ConsumerMessage, errorMessage string) []sarama.RecordHeader {

	headers := k.headerOperator.ExtractHeader(message)
	headers = k.headerOperator.AddIntoHeader(headers, ErrorKey, errorMessage)
	headers = k.headerOperator.AddIntoHeader(headers, RetryTopicKey, message.Topic)

	return headers
}
