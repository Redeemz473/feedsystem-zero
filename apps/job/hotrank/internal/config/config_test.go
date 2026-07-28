package config

import (
	"testing"

	"feedsystem-zero/common/eventx"
)

func TestValidateHotRankTopics(t *testing.T) {
	valid := Config{}
	valid.Kafka.Topics = []string{
		eventx.TopicInteractionLikeEvents,
		eventx.TopicInteractionCommentEvents,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	missing := Config{}
	missing.Kafka.Topics = []string{eventx.TopicInteractionLikeEvents}
	if err := missing.Validate(); err == nil {
		t.Fatal("Validate() expected missing topic error")
	}

	unexpected := Config{}
	unexpected.Kafka.Topics = []string{
		eventx.TopicInteractionLikeEvents,
		eventx.TopicFeedVideoEvents,
	}
	if err := unexpected.Validate(); err == nil {
		t.Fatal("Validate() expected unexpected topic error")
	}
}
