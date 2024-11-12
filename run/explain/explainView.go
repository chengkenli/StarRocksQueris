/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendView
 *@date    2024/8/28 14:43
 */

package explain

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strings"
)

func ExOlapOrView(db *gorm.DB, table string) string {
	// 模拟连接
	tb := strings.Split(table, ".")
	// 核实内表信息
	var schema map[string]interface{}
	r := db.Raw(fmt.Sprintf("select `TABLE_SCHEMA`,`TABLE_NAME`,`TABLE_TYPE`,`ENGINE` from information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", tb[0], tb[1])).Scan(&schema)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return table
	}

	if schema == nil {
		return table
	}

	if schema["TABLE_TYPE"].(string) == "VIEW" {
		var m []map[string]interface{}
		r := db.Raw("show create table " + table).Scan(&m)
		if r.Error != nil {
			util.Loggrs.Error(r.Error.Error())
			return table
		}
		if m == nil {
			return table
		}
		for _, m2 := range m {
			var stmt string
			cl1 := m2["Create View"]
			cl2 := m2["Create Materialized View"]
			if cl1 != nil {
				stmt = cl1.(string)
			}
			if cl2 != nil {
				stmt = cl2.(string)
			}
			// 查找匹配项
			match := regexp.MustCompile(`(?i)\s*FROM\s+([^\s.]+)\.([^\s.]+)`).FindStringSubmatch(stmt)
			if len(match) < 3 {
				util.Loggrs.Warn("length is short.")
				continue
			}
			return fmt.Sprintf("%s.%s", repler(match[1]), repler(match[2]))
		}
	}
	return table
}

func repler(str string) string {
	return strings.NewReplacer("`", "", ";", "").Replace(str)
}
