package behavioral

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"strconv"
	"time"
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
		kafka_wrapper.Logger.Printf("Message is not executed successfully: %+v so is routing to error topic: %+v, message: %+v\n", message.Topic, k.errorTopic)
		err = k.sendToErrorTopic(message, k.errorTopic, err.Error())
		if err != nil {
			kafka_wrapper.Logger.Printf("Message is not published to error topic: %+v\n", k.errorTopic)
		}
	}
	return err
}

func (k *retryBehaviour) sendToErrorTopic(message *sarama.ConsumerMessage, errorTopic string, errorMessage string) error {
	retryCount := getRetryCountFromHeader(message)
	retryCount++
	_, _, err := k.producer.SendMessage(&sarama.ProducerMessage{
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte(ErrorKey),
				Value: []byte(errorMessage),
			},
			{
				Key:   []byte(RetryKey),
				Value: []byte(strconv.Itoa(retryCount)),
			},
		},
		Topic: errorTopic,
		Value: sarama.StringEncoder(message.Value),
	})
	return err
}

func getRetryCountFromHeader(message *sarama.ConsumerMessage) int {
	for _, header := range message.Headers {
		if string(header.Key) == RetryKey {
			return getInt(header.Value)
		}
	}
	return 0
}

func getInt(s []byte) int {
	retryCountString := string(s)
	retryCount, _ := strconv.Atoi(retryCountString)
	return retryCount
}
