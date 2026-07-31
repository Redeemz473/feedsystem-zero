// Package httpclient — high level API convenience methods.
package httpclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// --- account ---

// Login authenticates with username+password and returns the parsed response.
// The bearer token is NOT stored on the client automatically; the caller
// decides how to distribute tokens across workers.
func (c *Client) Login(ctx context.Context, username, password string) (*LoginResp, error) {
	var out LoginResp
	if err := c.Do(ctx, http.MethodPost, "/account/login",
		nil, LoginReq{Username: username, Password: password}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetProfile fetches the currently authenticated user's profile.
func (c *Client) GetProfile(ctx context.Context) (*GetProfileResp, error) {
	var out GetProfileResp
	if err := c.Do(ctx, http.MethodGet, "/account/profile", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- video ---

// PublishVideo publishes a new video record. `play_url` and `cover_url` must
// have been produced by an earlier upload; the seed tool inserts placeholder
// file_assets rows for load tests to consume.
func (c *Client) PublishVideo(ctx context.Context, req PublishVideoReq) (*PublishVideoResp, error) {
	var out PublishVideoResp
	if err := c.Do(ctx, http.MethodPost, "/video/publish", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetVideo fetches a single video's detail (public endpoint, works anon).
func (c *Client) GetVideo(ctx context.Context, videoID uint64) (*GetVideoResp, error) {
	var out GetVideoResp
	if err := c.Do(ctx, http.MethodGet, "/video/"+strconv.FormatUint(videoID, 10), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- interaction ---

// LikeVideo toggles like=true on the given video for the current user.
func (c *Client) LikeVideo(ctx context.Context, videoID uint64) (*LikeVideoResp, error) {
	var out LikeVideoResp
	if err := c.Do(ctx, http.MethodPost, fmt.Sprintf("/interaction/video/%d/like", videoID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UnlikeVideo toggles like=false.
func (c *Client) UnlikeVideo(ctx context.Context, videoID uint64) (*UnlikeVideoResp, error) {
	var out UnlikeVideoResp
	if err := c.Do(ctx, http.MethodDelete, fmt.Sprintf("/interaction/video/%d/like", videoID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PublishComment posts a new comment on the video.
func (c *Client) PublishComment(ctx context.Context, videoID uint64, req PublishCommentReq) (*PublishCommentResp, error) {
	var out PublishCommentResp
	if err := c.Do(ctx, http.MethodPost, fmt.Sprintf("/interaction/video/%d/comments", videoID), nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListComments fetches a cursor-paged comment list.
func (c *Client) ListComments(ctx context.Context, videoID uint64, pageSize int) (*ListCommentsResp, error) {
	q := url.Values{}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out ListCommentsResp
	if err := c.Do(ctx, http.MethodGet, fmt.Sprintf("/interaction/video/%d/comments", videoID), q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- social ---

// Follow starts following targetUserID.
func (c *Client) Follow(ctx context.Context, targetUserID uint64) (*FollowResp, error) {
	var out FollowResp
	if err := c.Do(ctx, http.MethodPost, fmt.Sprintf("/social/users/%d/follow", targetUserID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Unfollow removes the follow relationship.
func (c *Client) Unfollow(ctx context.Context, targetUserID uint64) (*UnfollowResp, error) {
	var out UnfollowResp
	if err := c.Do(ctx, http.MethodDelete, fmt.Sprintf("/social/users/%d/follow", targetUserID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// IsFollowing reports whether the current user follows the target.
func (c *Client) IsFollowing(ctx context.Context, targetUserID uint64) (*IsFollowingResp, error) {
	var out IsFollowingResp
	if err := c.Do(ctx, http.MethodGet, fmt.Sprintf("/social/users/%d/following-status", targetUserID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- feed ---

// GetRecommendFeed reads the public recommend feed (anonymous allowed).
func (c *Client) GetRecommendFeed(ctx context.Context, pageSize int) (*FeedResp, error) {
	q := url.Values{}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out FeedResp
	if err := c.Do(ctx, http.MethodGet, "/feed/recommend", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetFollowingFeed reads the authenticated user's follow feed.
func (c *Client) GetFollowingFeed(ctx context.Context, pageSize int) (*FeedResp, error) {
	q := url.Values{}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out FeedResp
	if err := c.Do(ctx, http.MethodGet, "/feed/following", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetHotFeed reads the current hot ranking snapshot.
func (c *Client) GetHotFeed(ctx context.Context, snapshotAt, offset int64, pageSize int) (*HotFeedResp, error) {
	q := url.Values{}
	if snapshotAt > 0 {
		q.Set("snapshot_at", strconv.FormatInt(snapshotAt, 10))
	}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out HotFeedResp
	if err := c.Do(ctx, http.MethodGet, "/feed/hot", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- helpers for e2e that need the register/verification path ---

// SendVerification triggers a verification code email; used only by e2e and
// only when the caller has a way to fish the code out (e.g. Redis for tests).
func (c *Client) SendVerification(ctx context.Context, email string) (*VerificationResp, error) {
	var out VerificationResp
	if err := c.Do(ctx, http.MethodPost, "/account/verification",
		nil, VerificationReq{Email: email}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Register creates a new account.
func (c *Client) Register(ctx context.Context, req RegisterReq) (*RegisterResp, error) {
	var out RegisterResp
	if err := c.Do(ctx, http.MethodPost, "/account/register", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
