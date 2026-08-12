package logic

import (
	"bytes"
	"context"
	"testing"
	"time"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/interaction/interactionclient"
	"feedsystem-zero/apps/video/videoclient"

	"google.golang.org/grpc"
)

type concurrentVideoRPC struct {
	videoclient.Video
}

func (concurrentVideoRPC) BatchGetVideos(
	context.Context,
	*videoclient.BatchGetVideosReq,
	...grpc.CallOption,
) (*videoclient.BatchGetVideosResp, error) {
	return &videoclient.BatchGetVideosResp{Videos: []*videoclient.VideoInfo{{
		VideoId:  11,
		AuthorId: 22,
		Title:    "video",
	}}}, nil
}

type concurrentAccountRPC struct {
	accountclient.Account
	started chan<- struct{}
	release <-chan struct{}
}

func (rpc concurrentAccountRPC) BatchGetProfiles(
	ctx context.Context,
	_ *accountclient.BatchGetProfilesReq,
	_ ...grpc.CallOption,
) (*accountclient.BatchGetProfilesResp, error) {
	rpc.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rpc.release:
	}
	return &accountclient.BatchGetProfilesResp{Profiles: []*accountclient.PublicProfile{{
		UserId:   22,
		Username: "author",
	}}}, nil
}

type concurrentInteractionRPC struct {
	interactionclient.Interaction
	started chan<- struct{}
	release <-chan struct{}
}

func (rpc concurrentInteractionRPC) BatchGetVideoStats(
	ctx context.Context,
	_ *interactionclient.BatchGetVideoStatsReq,
	_ ...grpc.CallOption,
) (*interactionclient.BatchGetVideoStatsResp, error) {
	rpc.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rpc.release:
	}
	return &interactionclient.BatchGetVideoStatsResp{Stats: []*interactionclient.VideoInteractionStats{{
		VideoId:       11,
		LikesCount:    7,
		CommentsCount: 3,
		Popularity:    36,
		IsLiked:       true,
	}}}, nil
}

func TestValidateUploadedFileSignature(t *testing.T) {
	tests := []struct {
		name    string
		ext     string
		content []byte
		wantErr bool
	}{
		{name: "jpeg", ext: ".jpg", content: []byte{0xff, 0xd8, 0xff, 0x00}},
		{name: "png", ext: ".png", content: []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}},
		{name: "webp", ext: ".webp", content: []byte("RIFF0000WEBP")},
		{name: "mp4", ext: ".mp4", content: []byte{0, 0, 0, 0, 'f', 't', 'y', 'p', 0, 0, 0, 0}},
		{name: "webm", ext: ".webm", content: []byte{0x1a, 0x45, 0xdf, 0xa3}},
		{name: "spoofed extension", ext: ".mp4", content: []byte("not a video"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUploadedFileSignature(bytes.NewReader(tt.content), tt.ext)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateUploadedFileSignature() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadHTTPVideosByIDsEnrichesIndependentRPCsConcurrently(t *testing.T) {
	release := make(chan struct{})
	accountStarted := make(chan struct{}, 1)
	interactionStarted := make(chan struct{}, 1)

	type result struct {
		videos map[uint64]struct {
			username string
			likes    int64
			liked    bool
		}
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		videos, err := loadHTTPVideosByIDs(
			context.Background(),
			concurrentAccountRPC{started: accountStarted, release: release},
			concurrentVideoRPC{},
			concurrentInteractionRPC{started: interactionStarted, release: release},
			99,
			[]uint64{11},
		)
		converted := make(map[uint64]struct {
			username string
			likes    int64
			liked    bool
		}, len(videos))
		for id, video := range videos {
			converted[id] = struct {
				username string
				likes    int64
				liked    bool
			}{video.Authorusername, video.Likescount, video.Isliked}
		}
		resultCh <- result{videos: converted, err: err}
	}()

	waitStarted := func(name string, started <-chan struct{}) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s RPC did not start while the other enrichment RPC was blocked", name)
		}
	}
	waitStarted("account", accountStarted)
	waitStarted("interaction", interactionStarted)
	close(release)

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("loadHTTPVideosByIDs() error = %v", got.err)
		}
		video, ok := got.videos[11]
		if !ok {
			t.Fatal("video 11 missing")
		}
		if video.username != "author" || video.likes != 7 || !video.liked {
			t.Fatalf("unexpected enriched video: %+v", video)
		}
	case <-time.After(time.Second):
		t.Fatal("loadHTTPVideosByIDs() did not finish")
	}
}
