/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package api
 *@file    FeishuDb2OpenID
 *@date    2024/10/31 11:12
 */

package meta

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/robfig/cron/v3"
	"time"
)

var OpenIDCache = cache.New(24*time.Hour, 36*time.Hour)

// MetasOpenID 从数据表中根据userid拿到openid
func MetasOpenID() {
	// 企业专用，不公开
	return

	go metasInit()
	util.Loggrs.Info("初始化openid...")
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}

	var m []map[string]interface{}
	r := db.Raw("select * from ops.feishu_user_information").Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	var n int
	for _, m2 := range m {
		k, ok := m2["user_id"]
		v, vk := m2["open_id"]
		s, nk := m2["user_name"]
		if ok && vk && nk {
			OpenIDCache.Set(k.(string), fmt.Sprintf("%s:%s", v.(string), s.(string)), cache.DefaultExpiration)
			n++
		}
	}
	util.Loggrs.Info(fmt.Sprintf("初始化openid完成,总数:[%d],初始化:[%d].", len(m), n))
}

func SeriId(app, userid string) string {
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(err.Error())
		return ""
	}

	var m map[string]interface{}
	r := db.Raw(fmt.Sprintf("select * from ops.datalake_account_information where cluster_sort_name='%s' and account='%s'", app, userid)).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return ""
	}
	if m == nil {
		util.Loggrs.Warn("r is nil.")
		return ""
	}
	v, ok := m["user_id"]
	if ok {
		return v.(string)
	}
	return ""
}

// 定时刷新缓存
func metasInit() {
	crontab := cron.New()
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc("00 23 * * *", MetasOpenID)
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
