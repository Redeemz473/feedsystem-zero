package config

type Config struct {
	Name         string
	Mysql        MysqlConf
	BizRedis     RedisConf
	Upload       UploadConf
	AssetCleanup AssetCleanupConf
}

type MysqlConf struct {
	DataSource string
}

type RedisConf struct {
	Addr     string
	Password string `json:",optional"`
	DB       int    `json:",default=0"`
}

type UploadConf struct {
	Dir string
}

type AssetCleanupConf struct {
	BatchSize           int `json:",default=100"`
	PollIntervalSeconds int `json:",default=30"`
	GraceSeconds        int `json:",default=300"`
	ClaimTimeoutSeconds int `json:",default=300"`
	DeleteTimeoutMs     int `json:",default=5000"`
}
