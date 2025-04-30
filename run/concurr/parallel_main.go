/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package concurr
 *@file    parallel_main
 *@date    2024/11/15 17:24
 */

package concurr

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"gorm.io/gorm"
	"sync"
	"time"
)

func Parallel() {
	var wg sync.WaitGroup
	for i, m := range tools.UniqueMaps(util.ConnectBody) {
		app := m["app"].(string)
		wg.Add(1)
		go func(i int, app string) {
			defer wg.Done()

			db, err := conn.StarRocks(app)
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
			for _, ip := range frontends(db) {
				db, err := conn.StarRocksApp(app, ip)
				if err != nil {
					util.Loggrs.Error(err.Error())
					continue
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
				r := db.Raw("show processlist").Scan(&m)
				if r.Error != nil {
					util.Loggrs.Error(r.Error.Error())
					continue
				}
				for _, m2 := range m {
					if m2["Command"].(string) != "Query" {
						continue
					}

				}
			}

		}(i, app)
	}
	wg.Wait()
}

func frontends(db *gorm.DB) []string {
	var m []map[string]interface{}
	r := db.Raw("show frontends").Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return nil
	}
	var frontends []string
	for _, m2 := range m {
		frontends = append(frontends, m2["IP"].(string))
	}
	return frontends
}
