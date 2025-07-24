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
	"StarRocksQueris/pool"
	"StarRocksQueris/util"
	"context"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
	"net"
	"strings"
	"time"
)

var ipdb string

var cachelimit = pool.InstantiationCache("MailConnectionId", 30*time.Minute, 30*time.Minute)

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

		ticker := time.NewTicker(time.Hour * 1)
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
	util.Loggrs.Info("[ok] 初始化加载ipsystem缓存", len(util.ClientIPDec))
}

func GetclientItems(clientIp string) util.ClientIPData {
	clientip := strings.Split(clientIp, ":")[0]
	if val, ok := cachelimit.Get(clientip); ok {
		util.Loggrs.Info(clientip, " response:cache")
		return val.(util.ClientIPData)
	}
	domainname := ctxIp(clientip)
	if domainname != nil {
		item := getipname(strings.Split(domainname[0], ".")[0])
		go cachelimit.Set(clientip, item, cache.DefaultExpiration)
		util.Loggrs.Info(clientip, " response:actual")
		return item
	}
	return util.ClientIPData{}
}

func getipname(device_name string) util.ClientIPData {
	for _, data := range util.ClientIPDec {
		if data.ComputerName == device_name {
			return data
		}
	}
	return util.ClientIPData{}
}

func ctxIp(ip string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	domains, err := net.DefaultResolver.LookupAddr(ctx, ip)
	if err != nil {
		return nil
	}
	return domains
}
