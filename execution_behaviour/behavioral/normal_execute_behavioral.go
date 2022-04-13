package behavioral

import (
	"context"

	"github.com/Shopify/sarama"
	kafka_wrapper "github.com/Trendyol/kafka-wrapper"
)

type normalBehaviour struct {
	producer   sarama.SyncProducer
	executor   LogicOperator
	retryTopic string
}

func NormalBehavioral(producer sarama.SyncProducer, retryTopic string, executor LogicOperator) BehaviourExecutor {
	return &normalBehaviour{
		producer:   producer,
		executor:   executor,
		retryTopic: retryTopic,
	}
}

func (k *normalBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) (err error) {
	if err = k.executor.Operate(ctx, message); err != nil {
		kafka_wrapper.Logger.Printf("Have an error occurred while executing the logic: %+v, err:%+v\n", message.Topic, err)
		err = k.sendToRetryTopic(message, err)
		if err != nil {
			kafka_wrapper.Logger.Printf("Have an error occurred while publishing to retry topic: %+v , err:%+v\n", k.retryTopic, err)
		}
	}
	return err
}

func (k *normalBehaviour) sendToRetryTopic(message *sarama.ConsumerMessage, err error) error {
	headers := make([]sarama.RecordHeader, 0)
	for _, v := range message.Headers {
		headers = append(headers, *v)
	}

	headers = append(headers, sarama.RecordHeader{
		Key:   []byte(ErrorKey),
		Value: []byte(err.Error()),
	})

	_, _, sendError := k.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   k.retryTopic,
		Key:     sarama.StringEncoder(message.Key),
		Value:   sarama.StringEncoder(message.Value),
		Headers: headers,
	})
	return sendError
}
