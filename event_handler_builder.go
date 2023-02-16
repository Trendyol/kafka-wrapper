package kafka_wrapper

import (
	"github.com/Trendyol/kafka-wrapper/execution_behaviour"
)

type EventHandlerBuilder interface {
	AddBehavioralSelectors(topics TopicsParameters, selector execution_behaviour.BehavioralSelector) EventHandlerBuilder
	Build() map[string]execution_behaviour.BehavioralSelector
}

type eventHandlerBuilder struct {
	behavioralSelectors map[string]execution_behaviour.BehavioralSelector
}

func NewEventHandlerBuilder() EventHandlerBuilder {
	return &eventHandlerBuilder{
		behavioralSelectors: make(map[string]execution_behaviour.BehavioralSelector),
	}
}

func (e *eventHandlerBuilder) AddBehavioralSelectors(topics TopicsParameters, selector execution_behaviour.BehavioralSelector) EventHandlerBuilder {
	e.behavioralSelectors[topics.Topic] = selector
	e.behavioralSelectors[topics.RetryTopic] = selector
	e.behavioralSelectors[topics.ErrorTopic] = selector
	return e
}

func (e *eventHandlerBuilder) Build() map[string]execution_behaviour.BehavioralSelector {
	return e.behavioralSelectors
}
