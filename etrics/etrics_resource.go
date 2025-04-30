/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package etrics
 *@file    EtricsResourceGroupConstraint
 *@date    2024/10/22 9:46
 */

package etrics

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
	"sync"
	"time"
)

var vclass []string

func ResourceGroup(db *gorm.DB, username string) {
	if util.ConnectNorm.SlowQueryResourceGroupCpuCoreLimit <= 0 {
		return
	}
	if util.ConnectNorm.SlowQueryResourceGroupMemLimit <= 0 {
		return
	}
	if util.ConnectNorm.SlowQueryResourceGroupConcurrencyLimit <= 0 {
		return
	}

	if vclass == nil {
		util.Loggrs.Warn(uid, "资源组为空，初始化！")

		var mutex sync.Mutex
		vclass = constraint(db, &mutex)
	}
	if strings.Contains(strings.Join(vclass, ","), username) {
		return
	}
	util.Loggrs.Info(uid, fmt.Sprintf("开始 - 约束非白名单用户%s的并发度！", username))
	tf := time.Now().Format("060102150405")
	sql := fmt.Sprintf(`
		CREATE RESOURCE GROUP id%s
		TO(
			user='%s'
		)
		WITH(
			"cpu_core_limit"="%d",
			"mem_limit"="%d%%",
			"concurrency_limit"="%d"
		)`, tf, username,
		util.ConnectNorm.SlowQueryResourceGroupCpuCoreLimit,
		util.ConnectNorm.SlowQueryResourceGroupMemLimit,
		util.ConnectNorm.SlowQueryResourceGroupConcurrencyLimit)
	util.Loggrs.Info(uid, sql)
	//r := db.Exec(sql)
	r := db.Exec(sql)
	if r.Error != nil {
		util.Loggrs.Error(uid, r.Error.Error())
		return
	}
	util.Loggrs.Info(uid, fmt.Sprintf("结束 - 约束非白名单用户%s的并发度！[%s] - [%s]", username, tf, username))
}

func constraint(db *gorm.DB, mutex *sync.Mutex) []string {
	mutex.Lock()
	var m []map[string]interface{}
	r := db.Raw("SHOW RESOURCE GROUPS ALL").Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(uid, r.Error.Error())
		return nil
	}
	mutex.Unlock()

	var vc []string
	for _, m2 := range m {
		if v, ok := m2["classifiers"]; ok {
			vc = append(vc, v.(string))
		}
	}
	return vc
}

func CronRg() {
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	ticker := time.NewTicker(time.Minute * 1)
	for {
		select {
		case <-ticker.C:
			vclass = nil

			var mutex sync.Mutex
			vclass = constraint(db, &mutex)
			util.Loggrs.Info(uid, fmt.Sprintf("定时刷新 资源组结果集,length:%d", len(vclass)))
		}
	}
}
