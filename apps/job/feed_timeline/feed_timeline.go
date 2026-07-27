package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"

	"feedsystem-zero/apps/job/feed_timeline/internal/config"
	"feedsystem-zero/apps/job/feed_timeline/internal/logic"
	"feedsystem-zero/apps/job/feed_timeline/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/feed_timeline.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer := logic.NewTimelineConsumer(svcCtx)
	// 启动消费前先用 MySQL 当前状态完整构建全局最新流。
	// 之后 Kafka 中积压的事件再按最终状态幂等收敛，不会漏掉 Job 启动前的视频。
	if err := consumer.BootstrapGlobalTimeline(ctx); err != nil {
		logx.Must(err)
	}
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logx.Errorf("feed timeline consumer stopped with error: %v", err)
	}
}
