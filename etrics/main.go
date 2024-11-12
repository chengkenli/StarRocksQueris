/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package service
 *@file    main
 *@date    2024/10/21 10:25
 */

package etrics

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/robfig/cron/v3"
	"time"
)

var (
	StCache = cache.New(6*time.Hour, 12*time.Hour)
)

func Metrics() {
	crontab := cron.New()
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc("*/10 * * * *", StorageApp)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	// 启动定时器
	crontab.Start()
	// 定时任务是另起协程执行的,这里使用 select 简答阻塞.实际开发中需要
	// 根据实际情况进行控制
	select {}
}

func StorageApp() {
	var lark []*util.Larkbodys
	for _, m := range tools.UniqueMaps(util.ConnectBody) {
		app := m["app"].(string)
		_, b := StCache.Get(app)
		if b {
			util.Loggrs.Info(fmt.Sprintf("节点存储预警 ==> %s 集群存储告警，但由于定时缓存机制，本次告警将忽略！", app))
			continue
		}

		body := Storage(app)
		if body != nil {
			lark = append(lark, body)
		}
	}
	if lark != nil {
		robot.SendFsCartStorage(lark)
	}
}
