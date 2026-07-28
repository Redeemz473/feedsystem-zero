package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"

	"feedsystem-zero/apps/job/hotrank/internal/config"
	"feedsystem-zero/apps/job/hotrank/internal/logic"
	"feedsystem-zero/apps/job/hotrank/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/hotrank.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer := logic.NewHotRankConsumer(svcCtx)
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logx.Errorf("hotrank consumer stopped with error: %v", err)
	}
}
