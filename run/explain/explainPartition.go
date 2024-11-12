/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package explain
 *@file    explainPartition.go
 *@date    2024/8/28 17:42
 */

package explain

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

func IsPartitionMap(db *gorm.DB, schema []string) []map[string]int {
	var resultMap []map[string]int
	for _, table := range schema {
		/*获取分区信息*/
		var m []map[string]interface{}
		r := db.Raw(fmt.Sprintf("show partitions from %s", table)).Scan(&m)
		if r.Error != nil {
			util.Loggrs.Error(r.Error.Error())
			return nil
		}
		/*获取到有存储的分区*/
		var NulsName, NosName []string
		for _, s := range m {
			if s["DataSize"].(string) == ".000 " {
				NulsName = append(NulsName, fmt.Sprintf("[%s] [%s]", s["PartitionName"].(string), s["DataSize"].(string)))
				continue
			}
			if s["DataSize"].(string) == "0B" {
				NulsName = append(NulsName, fmt.Sprintf("[%s] [%s]", s["PartitionName"].(string), s["DataSize"].(string)))
				continue
			}
			NosName = append(NosName, fmt.Sprintf("[%s] [%s]", s["PartitionName"].(string), s["DataSize"].(string)))
		}

		tb := strings.Split(table, ".")
		resultMap = append(resultMap, map[string]int{tb[1]: len(NosName)})
	}
	return resultMap
}
