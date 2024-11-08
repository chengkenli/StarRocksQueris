/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package explain
 *@file    explainReplicaDistribution
 *@date    2024/8/20 16:28
 */

package explain

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

func ReplicaDistribution(db *gorm.DB, schema []string) ([]string, []string, error) {
	// 获取所有be信息
	var olap []string
	var n []map[string]interface{}
	r := db.Raw("show backends").Scan(&n)
	if r.Error != nil {
		return nil, nil, r.Error
	}
	//获取tablet分布百分比
	var tblist []string
	for _, table := range schema {
		if len(strings.Split(table, ".")) > 2 {
			continue
		}
		table := ExOlapOrView(db, table)
		var data []map[string]interface{}
		r := db.Raw("show data from " + table).Scan(&data)
		if r.Error != nil {
			util.Loggrs.Error(r.Error.Error())
			continue
		}
		// 查询副本分布情况
		var m []map[string]interface{}
		r = db.Raw(fmt.Sprintf("ADMIN SHOW REPLICA DISTRIBUTION FROM %s", table)).Scan(&m)
		if r.Error != nil {
			util.Loggrs.Error(r.Error.Error())
			continue
		}

		var tbrd []string
		for _, m2 := range m {
			var BackendHost string
			for _, m3 := range n {
				if m2["BackendId"].(string) == m3["BackendId"].(string) {
					BackendHost = m3["IP"].(string)
				}
			}
			msg := fmt.Sprintf("%-18s %-15s %-15s %-10s %-10s", BackendHost, m2["BackendId"].(string), m2["ReplicaNum"].(string), m2["Graph"].(string), m2["Percent"].(string))
			tbrd = append(tbrd, msg)
		}

		tblist = append(tblist, table)
		tblist = append(tblist, fmt.Sprintf("size:%v replicaCount:%v rowcount:%v", data[0]["Size"], data[0]["ReplicaCount"], data[0]["RowCount"]))
		tblist = append(tblist, tbrd...)
		tblist = append(tblist, "\n")
		olap = append(olap, table)
	}

	return tblist, olap, nil
}
