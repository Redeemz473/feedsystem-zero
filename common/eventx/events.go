package eventx

import "encoding/json"

const (
	EventTypeVideoPublished = "video.published"
	EventTypeVideoDeleted   = "video.deleted"

	EventTypeLikeCreated = "like.created"
	EventTypeLikeDeleted = "like.deleted"

	EventTypeCommentCreated = "comment.created"
	EventTypeCommentDeleted = "comment.deleted"

	EventTypeVideoStatDelta     = "video.stat.delta"
	EventTypeNotificationCreate = "notification.create"
	EventTypeNotificationDelete = "notification.delete"

	EventTypeFollowCreated = "follow.created"
	EventTypeFollowDeleted = "follow.deleted"
)

const (
	AggregateVideo        = "video"
	AggregateLike         = "like"
	AggregateComment      = "comment"
	AggregateNotification = "notification"
	AggregateFollow       = "follow"
)

const (
	LikeActionLike   = "like"
	LikeActionUnlike = "unlike"

	CommentActionCreate = "create"
	CommentActionDelete = "delete"

	FollowActionFollow   = "follow"
	FollowActionUnfollow = "unfollow"

	NotificationActionCreate = "create"
	NotificationActionDelete = "delete"
)

const (
	NotificationTypeVideoLike    = "video_like"
	NotificationTypeVideoComment = "video_comment"
	NotificationTypeFollow       = "follow"
)

// 热度权重是 interaction 在线统计与 hotrank-job 的共同业务口径。
// 修改权重时必须保持两条链路一致，避免实时统计和时间窗口榜单产生偏差。
const (
	LikePopularityWeight    int64 = 3
	CommentPopularityWeight int64 = 5
)

type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Producer      string          `json:"producer"`
	TraceID       string          `json:"trace_id,omitempty"`
	OccurredAt    int64           `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

type LikeEvent struct {
	EventID    string `json:"event_id"`
	RequestID  string `json:"request_id,omitempty"`
	VideoID    uint64 `json:"video_id"`
	UserID     uint64 `json:"user_id"`
	Action     string `json:"action"`
	Delta      int64  `json:"delta"`
	OccurredAt int64  `json:"occurred_at"`
}

type CommentEvent struct {
	EventID    string `json:"event_id"`
	RequestID  string `json:"request_id,omitempty"`
	CommentID  uint64 `json:"comment_id"`
	VideoID    uint64 `json:"video_id"`
	UserID     uint64 `json:"user_id"`
	Username   string `json:"username,omitempty"`
	Action     string `json:"action"`
	Delta      int64  `json:"delta"`
	OccurredAt int64  `json:"occurred_at"`
}

type VideoStatDeltaEvent struct {
	EventID       string `json:"event_id"`
	VideoID       uint64 `json:"video_id"`
	LikeDelta     int64  `json:"like_delta"`
	CommentDelta  int64  `json:"comment_delta"`
	PopularityAdd int64  `json:"popularity_add"`
	OccurredAt    int64  `json:"occurred_at"`
	SourceEventID string `json:"source_event_id,omitempty"`
}

type FeedVideoEvent struct {
	EventID    string   `json:"event_id"`
	VideoID    uint64   `json:"video_id"`
	AuthorID   uint64   `json:"author_id"`
	Action     string   `json:"action"`
	Tags       []string `json:"tags,omitempty"`
	OccurredAt int64    `json:"occurred_at"`
}

type NotificationEvent struct {
	EventID          string `json:"event_id"`
	SourceEventID    string `json:"source_event_id"`
	ReceiverID       uint64 `json:"receiver_id"`
	ActorID          uint64 `json:"actor_id"`
	VideoID          uint64 `json:"video_id,omitempty"`
	CommentID        uint64 `json:"comment_id,omitempty"`
	NotificationType string `json:"notification_type"`
	Action           string `json:"action"`
	OccurredAt       int64  `json:"occurred_at"`
}

// FollowEvent 单向关注关系事件（关注/取关）。
// follower_id 主动关注/取关 following_id。
type FollowEvent struct {
	EventID     string `json:"event_id"`
	FollowerID  uint64 `json:"follower_id"`
	FollowingID uint64 `json:"following_id"`
	Action      string `json:"action"` // "follow" | "unfollow"
	OccurredAt  int64  `json:"occurred_at"`
}
