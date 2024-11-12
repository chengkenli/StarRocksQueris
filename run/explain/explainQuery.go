/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package queryexplain
 *@file    explainQuery
 *@date    2024/8/19 15:23
 */

package explain

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
)

// ExplainQuery 执行计划分析
func ExplainQuery(db *gorm.DB, item *util.Process2, rangerMap []map[string]int) (*util.OlapScanExplain, error) {
	output := regexp.MustCompile(`/\*.*?\*/`).ReplaceAllString(item.Info, "")
	if strings.Contains(strings.ToLower(output), "refresh materialized view") {
		return nil, nil
	}

	var counts int
	var tables, partitions, cardinalitys []string

	for i := 0; i < 3; i++ {
		var count int
		var table, partition, cardinality []string
		var sql string
		if len(item.Db) != 0 {
			sql = fmt.Sprintf("use %s;explain %s", item.Db, output)
		} else {
			sql = fmt.Sprintf("explain %s", output)
		}
		var m []map[string]interface{}
		r := db.Raw(sql).Scan(&m)
		if r.Error != nil {
			return nil, r.Error
		}

		for _, m2 := range m {
			if strings.Contains(m2["Explain String"].(string), "TABLE:") {
				if len(strings.Split(m2["Explain String"].(string), ":")) >= 2 {
					table = append(table, strings.Split(m2["Explain String"].(string), ":")[1])
				}
			}
			if strings.Contains(m2["Explain String"].(string), "partitions=") {
				if len(strings.Split(m2["Explain String"].(string), "=")) >= 2 {
					partition = append(partition, strings.Split(m2["Explain String"].(string), "=")[1])
				}
			}
			if strings.Contains(m2["Explain String"].(string), "cardinality=") {
				if len(strings.Split(m2["Explain String"].(string), "=")) >= 2 {
					cardinality = append(cardinality, strings.Split(m2["Explain String"].(string), "=")[1])
				}
			}
			if strings.Contains(m2["Explain String"].(string), "cardinality=") {
				if len(strings.Split(m2["Explain String"].(string), "=")) >= 2 {
					c, _ := strconv.Atoi(strings.Split(m2["Explain String"].(string), "=")[1])
					count = count + c
				}
			}
		}
		if strings.Contains(strings.ToLower(output), "insert ") {
			table = table[1:]
		}

		if len(table) == 0 {
			continue
		}

		if strings.Contains(strings.ToLower(output), "iceberg.") && len(table) == len(cardinality) {
			counts = count
			tables = table
			partitions = partition
			cardinalitys = cardinality
			break
		}

		if len(table) == len(partition) && len(partition) == len(cardinality) {
			counts = count
			tables = table
			partitions = partition
			cardinalitys = cardinality
			break
		}
	}

	// 组合全新数组
	var info []string
	olapScan := true
	if strings.Contains(strings.ToLower(output), "iceberg.") {
		for i := 0; i < len(tables); i++ {
			info = append(info, fmt.Sprintf("#%d %s、(%s)", i, tables[i], cardinalitys[i]))
		}
	} else {
		for i := 0; i < len(tables); i++ {
			var scanPar string
			tx := strings.Split(partitions[i], "/")
			if len(tx) >= 2 {
				scan, _ := strconv.Atoi(tx[0])
				totl, _ := strconv.Atoi(tx[1])
				if rangerMap != nil {
					v := tools.RangerMap(tables[i], rangerMap)
					if tx[0] != tx[1] && scan != v {
						olapScan = false
					}
					if scan == v {
						if v == totl {
							scanPar = partitions[i]
						} else {
							scanPar = fmt.Sprintf("%s/%s/nN:%d", tx[0], tx[1], v)
						}
					} else {
						scanPar = partitions[i]
					}
				}
			} else {
				scanPar = partitions[i]
			}
			msg := fmt.Sprintf("#%d %s、(%s)、(%s)", i, tables[i], scanPar, cardinalitys[i])
			info = append(info, msg)
		}
	}

	return &util.OlapScanExplain{
		OlapCount:     counts,
		OlapScan:      olapScan,
		OlapPartition: info,
	}, nil
}
