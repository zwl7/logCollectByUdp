package main

import (
	"errors"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)


var DB *gorm.DB

// refer to https://github.com/ClickHouse/clickhouse-go

func initClickhouse() {
	dsn := "tcp://183.238.148.123:9000?database=default&username=123456&password=123456&read_timeout=5s&write_timeout=10s"
	err := errors.New("")
	DB, err = gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
}
