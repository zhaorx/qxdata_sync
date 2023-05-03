package main

import (
	"log"
	"runtime"
	"time"

	"github.com/robfig/cron/v3"
	"qxdata_sync/config"
	"qxdata_sync/job"
)

var cfg = config.Cfg

func main() {
	switch cfg.Mode {
	case "daily":
		// registerDailyJob() // 注册每日任务
	case "history":
		// 1. 根据 V_CD_WELL_SOURCE 建立taos中的每个井表
		job.NewOilSchJob().RunScheama()
		job.NewWaterSchJob().RunScheama()

		// // 2. 运行历史数据转储
		// job.NewOilHistJob().RunHistory()
		// job.NewWaterHistJob().RunHistory()
	}

	select {}
}

func registerDailyJob() {
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

// runNumGoroutineMonitor 打印协程数量
func runNumGoroutineMonitor() {
	log.Printf("协程数量->%d\n", runtime.NumGoroutine())

	for {
		select {
		case <-time.After(time.Second):
			log.Printf("协程数量->%d\n", runtime.NumGoroutine())
		}
	}
}
