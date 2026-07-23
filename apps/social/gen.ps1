# social-rpc 代码生成脚本
# ============================================================================
# 本仓库的 apps/social 已手写好以下文件（不依赖 protobuf 生成器）：
#   social.proto                       <- goctl 源
#   internal/config/config.go
#   internal/svc/servicecontext.go
#   internal/model/follow.go
#   internal/model/outbox.go
#   internal/server/socialserver.go     <- 引用 social.SocialServer（来自生成代码）
#   internal/logic/*.go                <- 5 个 logic 空壳，业务逻辑待填
#   socialclient/social.go            <- 引用 social.NewSocialClient（来自生成代码）
#   social.go                         <- 引用 social.RegisterSocialServer（来自生成代码）
#
# 但以下两个文件必须由工具链生成，当前仓库里【没有】，所以 social 包现在编译不过：
#   apps/social/social/social.pb.go
#   apps/social/social/social_grpc.pb.go
#
# 在有工具链的环境执行本脚本即可补齐，之后 `go build ./apps/social/...` 通过。
# ============================================================================
#
# 前置安装（任选其一，需 Go 环境）：
#   go install github.com/zeromicro/go-zero/tools/goctl@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   protoc 本体：https://github.com/protocolbuffers/protobuf/releases
#                下载后把 bin/protoc 加入 PATH
#   并确保 $GOPATH/bin 在 PATH（goctl / protoc-gen-go* 装在这里）
#
# 在项目根目录执行：
#   powershell -ExecutionPolicy Bypass -File apps/social/gen.ps1

$ErrorActionPreference = "Stop"

# goctl zrpc 模式会一次性产出：
#   social/social.pb.go + social/social_grpc.pb.go
#   socialclient/*（已手写则跳过）
#   internal/* 中缺失的文件（已手写的 logic/config/svc/model/server 不会被覆盖）
goctl rpc protoc `
  apps/social/social.proto `
  --go_out=apps/social/social `
  --go-grpc_out=apps/social/social `
  --zrpc_out=apps/social `
  --proto_path=.

Write-Host "done. generated social.pb.go + social_grpc.pb.go"
Write-Host "now run: go build ./apps/social/..."
