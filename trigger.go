/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package main
 *@file    trigger
 *@date    2024/11/11 15:09
 */

package main

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
	"time"
)

// 触发器
func tigger(db *gorm.DB, tablename string, c int) {
	var cannel = make(chan struct{})
	// 触发器，随时发现配置表是否发生更新
	go checksum(db, tablename, c, cannel)
	go commit(db, tablename, c, cannel)
}

func checksum(db *gorm.DB, tablename string, c int, cannel chan struct{}) {
	var caches = cache.New(10*time.Second, 10*time.Second)
	tick := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-tick.C:
			now := time.Now().Add(-10 * time.Second)
			var latestUpdate time.Time
			r := db.Raw(fmt.Sprintf("select updated_at from %s order by updated_at desc limit 1", tablename)).Scan(&latestUpdate)
			if r.Error != nil {
				util.Loggrs.Error(r.Error.Error())
				return
			}
			if latestUpdate.After(now) || latestUpdate.Equal(now) || now.Equal(latestUpdate) {
				if _, ok := caches.Get("sign"); ok {
					continue
				}
				// 表已被更新
				util.Loggrs.Info("reload   ... ", tablename, " updated_at!")
				cannel <- struct{}{}
				util.Loggrs.Info("It's done... ", tablename, " recovery~")
				util.Loggrs.Info(util.ConnectNorm.SlowQueryTime, ",", util.ConnectNorm.SlowQueryKtime, ",", util.ConnectNorm.SlowQueryConcurrencylimit)
				go commit(db, tablename, c, cannel)

				caches.Add("sign", latestUpdate, cache.DefaultExpiration)
			}
		}
	}
}

func commit(db *gorm.DB, tablename string, c int, cannel chan struct{}) {
	//////////////////////////////////////////
	Ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-cannel:
			return
		case <-Ticker.C:
			switch c {
			case 1:
				r := db.Raw(fmt.Sprintf("select * from %s where status > 0", tablename)).Scan(&util.ConnectRobot)
				if r.Error != nil {
					util.Loggrs.Error(r.Error.Error())
					<-cannel
					return
				}
			case 2:
				r := db.Raw(fmt.Sprintf("select * from %s where status > 0", tablename)).Scan(&util.ConnectBody)
				if r.Error != nil {
					util.Loggrs.Error(r.Error.Error())
					<-cannel
					return
				}
			case 3:
				r := db.Raw(fmt.Sprintf("select * from %s ", tablename)).Scan(&util.ConnectNorm)
				if r.Error != nil {
					util.Loggrs.Error(r.Error.Error())
					<-cannel
					return
				}
			}
		}
	}

}
