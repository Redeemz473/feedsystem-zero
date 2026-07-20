package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"

	"feedsystem-zero/apps/job/interaction_sync/internal/config"
	"feedsystem-zero/apps/job/interaction_sync/internal/logic"
	"feedsystem-zero/apps/job/interaction_sync/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/interaction_sync.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer := logic.NewSyncConsumer(svcCtx)
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logx.Errorf("interaction sync consumer stopped with error: %v", err)
	}
}
