/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_cron
 *@date    2024/11/8 17:28
 */

package pipe

import (
	"StarRocksQueris/util"
	"github.com/robfig/cron/v3"
)

func EmoCron() {
	// 单独队列方向
	go engine.OnGlobalQueries()
	// 主架构方式
	crontab := cron.New()
	if util.Config.GetString("mode.cronsyntax") == "" {
		util.Loggrs.Error("[fail].cron表达式不存在，轮询失败，需要先设置cronsyntax")
		return
	}
	util.Loggrs.Info("[ok] 初始化加载常驻模式:", util.Config.GetString("mode.cronsyntax"))
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc(util.Config.GetString("mode.cronsyntax"), func() {
		// job stsrt
		go EmoContext()
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
