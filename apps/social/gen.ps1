# social-rpc 代码生成脚本
# ============================================================================
# social.proto 是唯一 RPC 契约源。修改 proto 后运行本脚本，
# goctl 会更新 pb、grpc、client、server，并为新增 RPC 创建 logic 骨架。
# 已存在的业务 logic 不会被 goctl 覆盖。
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

Push-Location apps/social
try {
  goctl rpc protoc `
    social.proto `
    --go_out=. `
    --go-grpc_out=. `
    --zrpc_out=.
}
finally {
  Pop-Location
}

Write-Host "done. generated social.pb.go + social_grpc.pb.go"
Write-Host "now run: go build ./apps/social/..."
