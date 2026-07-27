// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import (
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Auth           AuthConf
	AccountRpc     zrpc.RpcClientConf
	VideoRpc       zrpc.RpcClientConf
	InteractionRpc zrpc.RpcClientConf
	SocialRpc      zrpc.RpcClientConf
	FeedRpc        zrpc.RpcClientConf
	BizRedis       RedisConf
	Mysql          MysqlConf
	Upload         UploadConf
}

type AuthConf struct {
	AccessSecret string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

type MysqlConf struct {
	DataSource string
}

type UploadConf struct {
	Dir                     string `json:",default=uploads"`
	PublicPrefix            string `json:",default=/uploads"`
	MaxVideoBytes           int64  `json:",default=104857600"`
	MaxCoverBytes           int64  `json:",default=10485760"`
	ChunkThresholdBytes     int64  `json:",default=20971520"`
	DefaultChunkBytes       int64  `json:",default=8388608"`
	MaxChunkBytes           int64  `json:",default=10485760"`
	ChunkSessionTTLSeconds  int64  `json:",default=86400"`
	UploadedFileTTLSeconds  int64  `json:",default=604800"`
	EnableInstantUpload     bool   `json:",default=true"`
	EnableChunkHashValidate bool   `json:",default=true"`
}
