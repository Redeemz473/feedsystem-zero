// Package httpclient — request/response structs mirroring apps/gateway/gateway.api.
//
// We keep them local (rather than importing gateway/internal/types) because
// the gateway types are generated from goctl and re-generating shouldn't
// force a rebuild of the load-test tool. Field names/tags must match the
// on-wire JSON exactly.
package httpclient

// --- account ---

type VerificationReq struct {
	Email string `json:"email"`
}
type VerificationResp struct {
	Verification string `json:"verification"`
}

type RegisterReq struct {
	Username     string `json:"username"`
	Password     string `json:"password"`
	Email        string `json:"email"`
	Verification string `json:"verification"`
}
type RegisterResp struct {
	Msg string `json:"msg"`
}

type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type LoginResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type GetProfileResp struct {
	UserID          uint64 `json:"user_id"`
	Username        string `json:"username"`
	Email           string `json:"email"`
	AvatarURL       string `json:"avatar_url"`
	Bio             string `json:"bio"`
	FollowersCount  int64  `json:"followers_count"`
	FollowingsCount int64  `json:"followings_count"`
}

// --- video ---

type VideoInfo struct {
	VideoID        uint64   `json:"video_id"`
	AuthorID       uint64   `json:"author_id"`
	AuthorUsername string   `json:"author_username"`
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	PlayURL        string   `json:"play_url"`
	CoverURL       string   `json:"cover_url"`
	LikesCount     int64    `json:"likes_count"`
	CommentsCount  int64    `json:"comments_count"`
	Popularity     int64    `json:"popularity"`
	Status         int32    `json:"status"`
	CreatedAt      int64    `json:"created_at"`
	UpdatedAt      int64    `json:"updated_at"`
	IsLiked        bool     `json:"is_liked"`
	Tags           []string `json:"tags"`
}

type PublishVideoReq struct {
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	PlayURL     string   `json:"play_url"`
	CoverURL    string   `json:"cover_url"`
	Tags        []string `json:"tags,omitempty"`
	RequestID   string   `json:"request_id,omitempty"`
}
type PublishVideoResp struct {
	Msg   string    `json:"msg"`
	Video VideoInfo `json:"video"`
}

type GetVideoResp struct {
	Video VideoInfo `json:"video"`
}

// --- interaction ---

type LikeVideoResp struct {
	Msg        string `json:"msg"`
	Liked      bool   `json:"liked"`
	LikesCount int64  `json:"likes_count"`
}
type UnlikeVideoResp = LikeVideoResp

type PublishCommentReq struct {
	Content   string `json:"content"`
	RequestID string `json:"request_id,omitempty"`
}
type CommentInfo struct {
	CommentID uint64 `json:"comment_id"`
	VideoID   uint64 `json:"video_id"`
	UserID    uint64 `json:"user_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	CanDelete bool   `json:"can_delete"`
}
type PublishCommentResp struct {
	Msg           string      `json:"msg"`
	Comment       CommentInfo `json:"comment"`
	CommentsCount int64       `json:"comments_count"`
}

type ListCommentsResp struct {
	Comments            []CommentInfo `json:"comments"`
	NextCursorCreatedAt int64         `json:"next_cursor_created_at"`
	NextCursorCommentID uint64        `json:"next_cursor_comment_id"`
	HasMore             bool          `json:"has_more"`
}

// --- social ---

type FollowResp struct {
	Msg      string `json:"msg"`
	Followed bool   `json:"followed"`
}
type UnfollowResp struct {
	Msg        string `json:"msg"`
	Unfollowed bool   `json:"unfollowed"`
}
type IsFollowingResp struct {
	Following bool `json:"following"`
}

// --- feed ---

type FeedResp struct {
	Videos                []VideoInfo `json:"videos"`
	NextCursorPublishedAt int64       `json:"next_cursor_published_at"`
	NextCursorVideoID     uint64      `json:"next_cursor_video_id"`
	HasMore               bool        `json:"has_more"`
}

type HotFeedVideo struct {
	Video    VideoInfo `json:"video"`
	HotScore float64   `json:"hot_score"`
	Rank     int64     `json:"rank"`
}
type HotFeedResp struct {
	Items      []HotFeedVideo `json:"items"`
	SnapshotAt int64          `json:"snapshot_at"`
	NextOffset int64          `json:"next_offset"`
	HasMore    bool           `json:"has_more"`
}
