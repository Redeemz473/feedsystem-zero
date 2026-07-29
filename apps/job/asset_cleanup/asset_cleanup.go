package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"

	"feedsystem-zero/apps/job/asset_cleanup/internal/config"
	"feedsystem-zero/apps/job/asset_cleanup/internal/logic"
	"feedsystem-zero/apps/job/asset_cleanup/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/asset_cleanup.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cleaner := logic.NewCleaner(svcCtx)
	if err := cleaner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logx.Errorf("asset cleanup stopped with error: %v", err)
	}
}
