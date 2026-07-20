package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"

	"feedsystem-zero/apps/job/outbox/internal/config"
	"feedsystem-zero/apps/job/outbox/internal/logic"
	"feedsystem-zero/apps/job/outbox/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/outbox.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dispatcher := logic.NewDispatcher(svcCtx)
	if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logx.Errorf("outbox dispatcher stopped with error: %v", err)
	}
}
