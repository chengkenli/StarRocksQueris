/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package clientip
 *@file    Run_ClientIP_Name
 *@date    2025/2/5 14:00
 */

package clientip

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"gorm.io/gorm"
	"time"
)

var ipdb string

func _init() {
	if util.Config.GetString("configdb.Schema.IpSystem") == "" {
		return
	}
	ipdb = util.Config.GetString("configdb.Schema.IpSystem")
	go func() {
		time.Sleep(time.Second * 5)
		db, err := conn.StarRocks(util.ConnectNorm.SlowQueryMetaapp)
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		if util.ClientIPDec == nil {
			getsign(db)
		}

		ticker := time.NewTicker(time.Minute * 5)
		for {
			select {
			case <-ticker.C:
				getsign(db)
			}
		}
	}()
}

func init() {
	_init()
}

func getsign(db *gorm.DB) {
	r := db.Raw("select * from " + ipdb).Scan(&util.ClientIPDec)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	//clear
	util.ClientDatas = nil
	for _, data := range util.ClientIPDec {
		util.ClientDatas = append(util.ClientDatas, data.Ip)
	}
	util.Loggrs.Info("[ok] 初始化加载ipsystem缓存", len(util.ClientDatas))
}
