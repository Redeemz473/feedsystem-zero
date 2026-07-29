package gormx

import (
	"os"
	"strconv"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	defaultMaxIdleConns    = 5
	defaultMaxOpenConns    = 10
	defaultConnMaxLifetime = time.Hour
	defaultConnMaxIdleTime = 10 * time.Minute
)

func NewDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 项目包含多个 RPC 和后台 Job。每个进程都开 100 条连接会很快耗尽
	// MySQL 的全局连接预算，因此默认值保持保守，并允许按进程通过环境变量覆盖。
	maxIdleConns := positiveIntFromEnv("FSZ_MYSQL_MAX_IDLE_CONNS", defaultMaxIdleConns)
	maxOpenConns := positiveIntFromEnv("FSZ_MYSQL_MAX_OPEN_CONNS", defaultMaxOpenConns)
	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetConnMaxLifetime(positiveDurationFromEnv("FSZ_MYSQL_CONN_MAX_LIFETIME_SECONDS", defaultConnMaxLifetime))
	sqlDB.SetConnMaxIdleTime(positiveDurationFromEnv("FSZ_MYSQL_CONN_MAX_IDLE_SECONDS", defaultConnMaxIdleTime))

	return db, nil
}

func positiveIntFromEnv(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func positiveDurationFromEnv(name string, fallback time.Duration) time.Duration {
	seconds := positiveIntFromEnv(name, int(fallback/time.Second))
	return time.Duration(seconds) * time.Second
}
