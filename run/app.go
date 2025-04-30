/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package run
 *@file    main
 *@date    2024/8/7 14:48
 */

package run

import (
	"StarRocksQueris/api"
	"StarRocksQueris/etrics"
	"StarRocksQueris/meta"
	fronends "StarRocksQueris/run/frontends"
	"StarRocksQueris/run/license"
	"StarRocksQueris/short"
	"StarRocksQueris/util"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"
)

func Run() {
	//go func() {
	//	// 使用默认的http.ServeMux，pprof路由已经注册
	//	if err := http.ListenAndServe(":6060", nil); err != nil {
	//		util.Loggrs.Error(err.Error())
	//	}
	//}()
	api.InitFeiShu()
	err := os.Mkdir(fmt.Sprintf("%s/sql/", util.LogPath), 0755)
	if err != nil {
	}
	if util.P.Check {
		util.ConnectNorm.SlowQueryTime = 15
		fronends.EmoContext()
		time.Sleep(time.Second * 5)
		return
	}
	ch := make(chan struct{})
	util.Loggrs.Info("[main].start app.")
	go fronends.EmoCron()
	go etrics.CronRg()
	go etrics.Metrics()
	go fronends.TFIDFCRON()
	go license.Sessionlicense()
	go meta.MetasOpenID()
	go short.ShortQueryApp()
	// 初始化定时任务
	<-ch
}
