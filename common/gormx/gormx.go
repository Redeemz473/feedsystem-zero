package gormx

import (
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dsn string) (*gorm.DB, error) {
	//初始化一个数据库连接
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(10)                  //连接池的空闲连接
	sqlDB.SetMaxOpenConns(100)                 //mysql的最大连接数
	sqlDB.SetConnMaxLifetime(time.Hour)        //连接最长存活时间
	sqlDB.SetConnMaxIdleTime(10 * time.Minute) //连接最长空闲时间

	return db, nil
}
