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
	if util.ConnectNorm.SlowQueryMetaapp == "" {
		return
	}
	larkmeta := util.Config.GetString("configdb.Schema.LarkMeta")
	if larkmeta == "" {
		return
	}
	//slow_query_metaapp
	// 企业专用，不公开
	go metasInit()
	db, err := conn.StarRocks(util.ConnectNorm.SlowQueryMetaapp)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()

	var m []map[string]interface{}
	r := db.Raw("select * from " + larkmeta).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	var n int
	for _, m2 := range m {
		OpenIDCache.Set(m2["user_id"].(string), fmt.Sprintf("%s:%s", m2["open_id"].(string), m2["user_name"].(string)), cache.DefaultExpiration)
		n++
	}
	util.Loggrs.Info(fmt.Sprintf("[ok] 初始化加载openid缓存,总数:[%d],初始化:[%d].", len(m), n))
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
