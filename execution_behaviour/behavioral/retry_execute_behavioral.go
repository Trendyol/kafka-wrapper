package behavioral

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/Trendyol/kafka-wrapper/utils"
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
	loggerHelper   *params.LoggerHelper
}

func RetryBehavioral(producer sarama.SyncProducer, errorTopic string, executor LogicOperator, retryCount int, headerOperator utils.HeaderOperation, logger params.Logger) BehaviourExecutor {
	return &retryBehaviour{
		producer:       producer,
		executor:       executor,
		retryCount:     retryCount,
		errorTopic:     errorTopic,
		headerOperator: headerOperator,
		loggerHelper:   params.NewLoggerHelper(logger),
	}
}

func (k *retryBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) (err error) {

	for i := 0; i < k.retryCount; i++ {
		if i == 0 {
			latestExecutableTime := time.Now().Add(time.Duration(-5) * time.Second)
			if latestExecutableTime.Before(message.Timestamp) {
				sleepTime := message.Timestamp.Sub(latestExecutableTime)
				k.loggerHelper.Info(ctx, "System will sleep for %+v", sleepTime)
				time.Sleep(sleepTime)
			} else {
				k.loggerHelper.Info(ctx, "No need to sleep for message time %+v", message.Timestamp)
			}
		} else {
			time.Sleep(250 * time.Millisecond)
		}

		err = k.executor.Operate(ctx, message)
		if err == nil {
			k.loggerHelper.Info(ctx, "Message is executed successfully, message key: %+v", message.Key)
			break
		}
	}

	if err != nil {
		k.loggerHelper.Error(ctx, "Message is not executed successfully: %+v so routing it to the error topic: %+v. err: %+v", message.Topic, k.errorTopic, err)
		err = k.sendToErrorTopic(message, err.Error())
		if err != nil {
			k.loggerHelper.Error(ctx, "Message is not published to the error topic: %+v. err: %+v", k.errorTopic, err)
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
