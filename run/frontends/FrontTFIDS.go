/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendTFIDS
 *@date    2024/8/21 21:59
 */

package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/robfig/cron/v3"
	"time"
)

var STARRROCKS_OLAP_QUERYID_STMT []map[string]interface{}
var tfCache = cache.New(1*time.Hour, 6*time.Hour)

// TFIDFCRON 定时扫描全局表
func TFIDFCRON() {
	go func() {
		crontab := cron.New()
		// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
		_, err := crontab.AddFunc("08 */1 * * *", func() {
			// job stsrt
			tfidf()
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
	}()
}

func tfidf() {
	if !tools.AuthRegis() {
		return
	}

	db, err := conn.StarRocksItem(
		&tools.SrAvgs{
			Host: util.ConnectNorm.SlowQueryDataRegistrationHost,
			Port: util.ConnectNorm.SlowQueryDataRegistrationPort,
			User: util.ConnectNorm.SlowQueryDataRegistrationUsername,
			Pass: util.ConnectNorm.SlowQueryDataRegistrationPassword,
		})
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	sql := "select queryId,stmt from " + util.ConnectNorm.SlowQueryDataRegistrationTable
	r := db.Raw(sql).Scan(&STARRROCKS_OLAP_QUERYID_STMT)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	tfCache.Set("sign", r, cache.DefaultExpiration)
}

func TFIDF(stmt string) []string {
	_, ok := tfCache.Get("sign")
	if !ok {
		tfidf()
	}
	var result []string
	for i := 0; i < len(STARRROCKS_OLAP_QUERYID_STMT); i++ {
		TargetQueryId := STARRROCKS_OLAP_QUERYID_STMT[i]["queryId"].(string)
		TargetStmt := STARRROCKS_OLAP_QUERYID_STMT[i]["stmt"].(string)
		p := SchemaTFIDF(stmt, TargetStmt)
		if p >= 90 {
			result = append(result, fmt.Sprintf("%s(%0.1f%%)", TargetQueryId, p))
		}
	}
	return tools.RemoveDuplicateStrings(result)
}
