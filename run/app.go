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
	"StarRocksQueris/run/license"
	pipe "StarRocksQueris/run/pipe"
	"StarRocksQueris/util"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"
)

func Run() {

	api.InitFeiShu()
	err := os.Mkdir(fmt.Sprintf("%s/sql/", util.LogPath), 0755)
	if err != nil {
	}
	if util.P.Check {
		util.ConnectNorm.SlowQueryTime = 1800
		pipe.EmoContext()
		time.Sleep(time.Second * 5)
		return
	}
	ch := make(chan struct{})
	util.Loggrs.Info("[main].start app.")
	go pipe.EmoCron()
	go etrics.CronRg()
	go etrics.Metrics()
	go pipe.TFIDFCRON()
	go license.Sessionlicense()
	go meta.MetasOpenID()
	//go short.ShortQueryApp()
	// 初始化定时任务
	<-ch
}
