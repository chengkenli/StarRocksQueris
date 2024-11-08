/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_cron
 *@date    2024/11/8 17:28
 */

package fronends

import (
	"StarRocksQueris/util"
	"github.com/robfig/cron/v3"
)

func EmoCron() {
	util.Loggrs.Info("[cron].进入常驻模式")
	crontab := cron.New()
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc("*/2 * * * *", func() {
		// job stsrt
		go EmoIndex()
		// job end
	})
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
