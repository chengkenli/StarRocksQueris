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
	"time"
)

var vclass []string

func ResourceGroup(db *gorm.DB, username string) {
	if util.ConnectNorm["slow_query_resource_group_cpu_core_limit"] == nil {
		return
	}
	if util.ConnectNorm["slow_query_resource_group_mem_limit"] == nil {
		return
	}
	if util.ConnectNorm["slow_query_resource_group_concurrency_limit"] == nil {
		return
	}

	if vclass == nil {
		util.Loggrs.Warn("资源组为空，初始化！")
		vclass = constraint(db)
	}
	if strings.Contains(strings.Join(vclass, ","), username) {
		return
	}
	util.Loggrs.Info(fmt.Sprintf("开始 - 约束非白名单用户%s的并发度！", username))
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
		util.ConnectNorm["slow_query_resource_group_cpu_core_limit"].(int64),
		util.ConnectNorm["slow_query_resource_group_mem_limit"].(int64),
		util.ConnectNorm["slow_query_resource_group_concurrency_limit"].(int64))
	util.Loggrs.Info(sql)
	//r := db.Exec(sql)
	r := db.Exec(sql)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	util.Loggrs.Info(fmt.Sprintf("结束 - 约束非白名单用户%s的并发度！[%s] - [%s]", username, tf, username))
}

func constraint(db *gorm.DB) []string {
	var m []map[string]interface{}
	r := db.Raw("SHOW RESOURCE GROUPS ALL").Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return nil
	}
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
		util.Loggrs.Error(err.Error())
		return
	}
	ticker := time.NewTicker(time.Minute * 1)
	for {
		select {
		case <-ticker.C:
			vclass = nil
			vclass = constraint(db)
			util.Loggrs.Info(fmt.Sprintf("定时刷新 资源组结果集,length:%d", len(vclass)))
		}
	}
}
