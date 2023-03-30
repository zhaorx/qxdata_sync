package main

import (
	"github.com/robfig/cron/v3"
	"qxdata_sync/config"
	_ "qxdata_sync/config"
	_ "qxdata_sync/database"
	"qxdata_sync/job"
)

var cfg = config.Cfg

func main() {
	registerJob() // 注册cron

	// 2. init config 根据config里时间段 顺序转储油井日数据到taos中

	if len(cfg.HistoryStart) > 0 && len(cfg.HistoryEnd) > 0 {
		go job.RunHistory()
	}

	select {}
}

func registerJob() {
	if len(cfg.Cron) == 0 {
		panic("Cron表达式为空!")
	}

	c := newWithSeconds()
	_, err := c.AddFunc(cfg.Cron, func() {
		job.RunDaily()
	})
	if err != nil {
		panic(err)
	}
	c.Start()
}

// 返回一个支持至 秒 级别的 cron
func newWithSeconds() *cron.Cron {
	secondParser := cron.NewParser(cron.Second | cron.Minute |
		cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor)
	return cron.New(cron.WithParser(secondParser), cron.WithChain())
}
