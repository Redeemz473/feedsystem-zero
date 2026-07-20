// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"feedsystem-zero/apps/gateway/internal/config"
	"feedsystem-zero/apps/gateway/internal/handler"
	"feedsystem-zero/apps/gateway/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/gateway.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	registerUploadFileServer(server, c.Upload)
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}

func registerUploadFileServer(server *rest.Server, upload config.UploadConf) {
	prefix := strings.TrimRight(upload.PublicPrefix, "/")
	if prefix == "" {
		prefix = "/uploads"
	}

	server.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   prefix + "/:category/:filename",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			rel := strings.TrimPrefix(r.URL.Path, prefix+"/")
			clean := filepath.Clean(rel)
			if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
				http.NotFound(w, r)
				return
			}

			http.ServeFile(w, r, filepath.Join(upload.Dir, clean))
		},
	})
}
