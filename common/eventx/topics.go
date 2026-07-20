package eventx

const (
	TopicInteractionLikeEvents    = "interaction.like.events"
	TopicInteractionCommentEvents = "interaction.comment.events"
	TopicVideoStatDeltaEvents     = "video.stat.delta.events"
	TopicFeedVideoEvents          = "feed.video.events"
	TopicNotificationEvents       = "notification.events"
)

const (
	ConsumerLikeSync      = "like-sync-job"
	ConsumerCommentSync   = "comment-sync-job"
	ConsumerVideoStatSync = "video-stat-sync-job"
	ConsumerHotRank       = "hotrank-job"
	ConsumerFeedTimeline  = "feed-timeline-job"
	ConsumerNotification  = "notification-job"
)

func AllTopics() []string {
	return []string{
		TopicInteractionLikeEvents,
		TopicInteractionCommentEvents,
		TopicVideoStatDeltaEvents,
		TopicFeedVideoEvents,
		TopicNotificationEvents,
	}
}
