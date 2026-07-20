// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListCommentsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListCommentsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListCommentsLogic {
	return &ListCommentsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListCommentsLogic) ListComments(req *types.ListCommentsReq) (resp *types.ListCommentsResp, err error) {
	rpcResp, err := l.svcCtx.InteractionRpc.ListComments(l.ctx, &interactionclient.ListCommentsReq{
		VideoId:         req.Videoid,
		ViewerId:        optionalUserIDFromCtx(l.ctx),
		CursorCreatedAt: req.Cursorcreatedat,
		CursorCommentId: req.Cursorcommentid,
		PageSize:        req.Pagesize,
	})
	if err != nil {
		return nil, err
	}

	comments := make([]types.CommentInfo, 0, len(rpcResp.GetComments()))
	for _, item := range rpcResp.GetComments() {
		comments = append(comments, toHTTPCommentInfo(item))
	}

	return &types.ListCommentsResp{
		Comments:            comments,
		Nextcursorcreatedat: rpcResp.GetNextCursorCreatedAt(),
		Nextcursorcommentid: rpcResp.GetNextCursorCommentId(),
		Hasmore:             rpcResp.GetHasMore(),
	}, nil
}
