package config

type Config struct {
	Name         string
	Mysql        MysqlConf
	EventCleanup EventCleanupConf
}

type MysqlConf struct {
	DataSource string
}

type EventCleanupConf struct {
	BatchSize                int `json:",default=100"`
	MaxBatchesPerRun         int `json:",default=20"`
	PollIntervalSeconds      int `json:",default=300"`
	DeleteTimeoutMs          int `json:",default=5000"`
	BatchIntervalMs          int `json:",default=200"`
	MaxRunSeconds            int `json:",default=30"`
	OutboxSentRetentionHours int `json:",default=168"`
	// 0 表示保留死信，等待人工审计或重放；生产环境确认已归档后再配置保留时长。
	DeadLetterRetentionHours int `json:",default=0"`
}
