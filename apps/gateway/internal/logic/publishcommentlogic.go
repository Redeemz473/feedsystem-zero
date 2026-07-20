// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"strings"

	"feedsystem-zero/apps/account/accountclient"
	"feedsystem-zero/apps/gateway/internal/svc"
	"feedsystem-zero/apps/gateway/internal/types"
	"feedsystem-zero/apps/interaction/interactionclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublishCommentLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPublishCommentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PublishCommentLogic {
	return &PublishCommentLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *PublishCommentLogic) PublishComment(req *types.PublishCommentReq) (resp *types.PublishCommentResp, err error) {
	userID, err := userIDFromCtx(l.ctx)
	if err != nil {
		return nil, err
	}

	requestID := strings.TrimSpace(req.Requestid)
	if requestID == "" {
		requestID, err = gatewayGeneratedRequestID(userID, req.Videoid)
		if err != nil {
			return nil, status.Error(codes.Internal, "生成评论幂等ID失败")
		}
	}

	profile, err := l.svcCtx.AccountRpc.GetProfile(l.ctx, &accountclient.GetProfileReq{
		UserId: userID,
	})
	if err != nil {
		return nil, err
	}

	rpcResp, err := l.svcCtx.InteractionRpc.PublishComment(l.ctx, &interactionclient.PublishCommentReq{
		UserId:    userID,
		Username:  profile.GetUsername(),
		VideoId:   req.Videoid,
		Content:   req.Content,
		RequestId: requestID,
	})
	if err != nil {
		return nil, err
	}

	return &types.PublishCommentResp{
		Msg:           rpcResp.GetMsg(),
		Comment:       toHTTPCommentInfo(rpcResp.GetComment()),
		Commentscount: rpcResp.GetCommentsCount(),
	}, nil
}
