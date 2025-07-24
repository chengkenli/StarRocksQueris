/*
 *@author  chengkenli
 *@project StarRocksRM
 *@package app
 *@file    mian
 *@date    2024/10/17 10:19
 */

package short

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

var healthOpen = cache.New(2*time.Hour, 4*time.Hour)
var onClose = cache.New(24*time.Hour, 24*time.Hour)
var lastTime = cache.New(24*time.Hour, 24*time.Hour)
var lastcache = cache.New(1*time.Hour, 1*time.Hour)
var last5min = cache.New(5*time.Minute, 5*time.Minute)
var applist []string

func ShortQueryApp() {
	if util.Config.GetString("configdb.Schema.ShortQuery") == "" {
		return
	}
	shortdb = util.Config.GetString("configdb.Schema.ShortQuery")
	logsrus()

	var cannel = make(chan struct{})

	j := Job{
		Lark:   make(chan *util.Larkbodys),
		Donec:  make(chan string),
		Signal: make(chan string),
	}
	go func() {
		for {
			select {
			case lark := <-j.Lark:
				robot.SendFsCartShortQuery(lark)
				loggrs.Info(uid, "send feishu done.")
			}
		}
	}()

	// 触发器，随时发现配置表是否发生更新
	go j.trigger(util.Connect, cannel)
	go j.Conmit(util.Connect, cannel)

	ch := make(chan int)
	<-ch
}

// 判断当前时间是否在时间区间内
func isTime(ctime string) (string, bool) {
	split := strings.Split(ctime, "-")
	// 获取当前时间
	now := time.Now()
	// 设置时间范围的开始和结束时间
	ssH, _ := strconv.Atoi(strings.Split(split[0], ":")[0])
	ssM, _ := strconv.Atoi(strings.Split(split[0], ":")[1])
	startHour, startMinute := ssH, ssM
	esH, _ := strconv.Atoi(strings.Split(split[1], ":")[0])
	esM, _ := strconv.Atoi(strings.Split(split[1], ":")[1])
	endHour, endMinute := esH, esM
	// 创建一个Location对象，通常使用Local表示本地时区
	loc := time.Local
	// 根据当前日期和设置的小时分钟创建时间对象
	start := time.Date(now.Year(), now.Month(), now.Day(), startHour, startMinute, 0, 0, loc)
	end := time.Date(now.Year(), now.Month(), now.Day(), endHour, endMinute, 0, 0, loc)
	//end
	return split[1], tools.IsTimeWithinRange(now, start, end)
}
