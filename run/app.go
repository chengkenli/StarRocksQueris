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
	"StarRocksQueris/util"
	"fmt"
	"os"
)

func Run() {
	api.InitFeiShu()
	os.Mkdir(fmt.Sprintf("%s/sql/", util.LogPath), 0755)
	if util.P.Check {
		fronends.EmoIndex()
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
	// 初始化定时任务
	<-ch
}
