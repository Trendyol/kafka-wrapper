package behavioral

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Trendyol/kafka-wrapper"
	"github.com/Trendyol/kafka-wrapper/slack_api_client"
)

type errorBehaviour struct {
	client             slack_api_client.Client
	notificationFormat string
}

func ErrorBehavioral(client slack_api_client.Client, title string) BehaviourExecutor {
	return &errorBehaviour{
		client:             client,
		notificationFormat: "*************** " + title + " *************** \n EVENT: %v \n :alert: %v ",
	}
}

func (k *errorBehaviour) Process(ctx context.Context, message *sarama.ConsumerMessage) error {
	errorMessage := getErrorMessageFromHeaders(message)
	err := k.client.PushNotification(fmt.Sprintf(k.notificationFormat, string(message.Value), errorMessage))
	if err != nil {
		kafka_wrapper.Logger.Println("Slack publish error, err:", err)
	}
	return nil
}

func getErrorMessageFromHeaders(message *sarama.ConsumerMessage) string {
	for _, header := range message.Headers {
		if string(header.Key) == ErrorKey {
			return string(header.Value)
		}
	}
	return ""
}
