package params_test

import (
	"github.com/Trendyol/kafka-wrapper/params"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_it_should_join_given_parameters(t *testing.T) {
	// Given
	topic_1 := params.TopicsParameters{
		Topic: "topic_1",
	}
	topic_2 := params.TopicsParameters{
		Topic: "topic_2",
	}
	topic_3 := params.TopicsParameters{
		Topic: "topic_3",
	}

	// When
	aggregatedTopics := params.FromTopics(topic_1, topic_2, topic_3)

	// Then
	assert.Len(t, aggregatedTopics, 3)
	assert.Equal(t, topic_1.Topic, aggregatedTopics[0].Topic)
	assert.Equal(t, topic_2.Topic, aggregatedTopics[1].Topic)
	assert.Equal(t, topic_3.Topic, aggregatedTopics[2].Topic)
}

func Test_it_should_join_topic_names_except_the_shared_error_topic(t *testing.T) {
	// Given
	topic_1 := params.TopicsParameters{
		Topic:                 "topic_1",
		ErrorTopic:            "errtopic_1",
		RetryTopic:            "retrytopic_1",
		UsingSharedErrorTopic: false,
	}
	topic_2 := params.TopicsParameters{
		Topic:                 "topic_2",
		ErrorTopic:            "errtopic_2",
		RetryTopic:            "retrytopic_2",
		UsingSharedErrorTopic: true,
	}

	// When
	joinedTopicNames := params.JoinAllTopics([]params.TopicsParameters{topic_1, topic_2})

	// Then
	assert.Len(t, joinedTopicNames, 5)
	assert.Equal(t, topic_1.ErrorTopic, joinedTopicNames[0])
	assert.Equal(t, topic_1.RetryTopic, joinedTopicNames[1])
	assert.Equal(t, topic_1.Topic, joinedTopicNames[2])
	assert.Equal(t, topic_2.RetryTopic, joinedTopicNames[3])
	assert.Equal(t, topic_2.Topic, joinedTopicNames[4])
}
