/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package explain
 *@file    explainCosts
 *@date    2024/11/15 14:06
 */

package explain

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strings"
	"time"
)

// Explaincosts  查询计划
func Explaincosts(db *gorm.DB, item *util.Process2) string {
	return ""
	output := regexp.MustCompile(`/\*.*?\*/`).ReplaceAllString(item.Info, "")
	if strings.Contains(strings.ToLower(output), "refresh materialized view") {
		return ""
	}
	if strings.Contains(item.Info, "iceberg.") {
		util.Loggrs.Info(item.Id, " SQL中包含iceberg，跳出！")
		return ""
	}
	if strings.Contains(strings.ToLower(item.Info), "insert") && strings.Contains(strings.ToLower(item.Info), "into") {
		util.Loggrs.Info(item.Id, " SQL中包含insert into，跳出！")
		return ""
	}

	exlogfile := fmt.Sprintf("%s/sql/explain_%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	go func() {
		var sql string
		if len(item.Db) != 0 {
			sql = fmt.Sprintf("USE %s;EXPLAIN %s", item.Db, output)
		} else {
			sql = fmt.Sprintf("EXPLAIN %s", output)
		}
		var m []map[string]interface{}
		r := db.Raw(sql).Scan(&m)
		if r.Error != nil {
			return
		}
		if m == nil {
			return
		}
		var msg []string
		for _, m2 := range m {
			msg = append(msg, m2["Explain String"].(string))
		}
		tools.WriteFile(exlogfile, strings.Join(msg, "\n"))
	}()
	return exlogfile
}
