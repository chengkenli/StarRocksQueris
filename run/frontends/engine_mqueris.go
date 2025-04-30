/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnBigQuery
 *@date    2024/9/3 17:03
 */

package fronends

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

func Queris(db *gorm.DB, m2 *util.Querisign) util.SessionBigQuery {
	// 实施
	var nodes []string
	var cm []map[string]interface{}
	r := db.Raw(fmt.Sprintf("SHOW PROC '/current_queries/%s/hosts'", m2.QueryId)).Scan(&cm)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return util.SessionBigQuery{
			Db:            db,
			StartTime:     m2.StartTime,
			QueryId:       m2.QueryId,
			ConnectionId:  m2.ConnectionId,
			Database:      m2.Database,
			User:          m2.User,
			ScanBytes:     m2.ScanBytes,
			ScanRows:      m2.ScanRows,
			MemoryUsage:   m2.MemoryUsage,
			DiskSpillSize: m2.DiskSpillSize,
			CPUTime:       m2.DiskSpillSize,
			ExecTime:      m2.ExecTime,
		}
	}
	nodes = append(nodes, fmt.Sprintf("  %-2s %-20s %-15s %-15s %-15s %-15s", "ID", "Host", "ScanBytes", "ScanRows", "MemUsageBytes", "CpuCostSeconds"))
	for i, m3 := range cm {
		msg := fmt.Sprintf("> %-2d %-20s %-15s %-15s %-15s %-15s ", i,
			m3["Host"].(string),
			m3["ScanBytes"].(string),
			strings.NewReplacer(" rows", "").Replace(m3["ScanRows"].(string)),
			m3["MemUsageBytes"].(string),
			m3["CpuCostSeconds"].(string),
		)
		nodes = append(nodes, msg)
	}

	var dm map[string]interface{}
	r = db.Raw(fmt.Sprintf("SHOW PROC '/current_queries/%s'", m2.QueryId)).Scan(&dm)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return util.SessionBigQuery{
			Db:            db,
			StartTime:     m2.StartTime,
			QueryId:       m2.QueryId,
			ConnectionId:  m2.ConnectionId,
			Database:      m2.Database,
			User:          m2.User,
			ScanBytes:     m2.ScanBytes,
			ScanRows:      m2.ScanRows,
			MemoryUsage:   m2.MemoryUsage,
			DiskSpillSize: m2.DiskSpillSize,
			CPUTime:       m2.DiskSpillSize,
			ExecTime:      m2.ExecTime,
			Nodes:         nodes,
		}
	}

	return util.SessionBigQuery{
		Db:            db,
		StartTime:     m2.StartTime,
		QueryId:       m2.QueryId,
		ConnectionId:  m2.ConnectionId,
		Database:      m2.Database,
		User:          m2.User,
		ScanBytes:     m2.ScanBytes,
		ScanRows:      m2.ScanRows,
		MemoryUsage:   m2.MemoryUsage,
		DiskSpillSize: m2.DiskSpillSize,
		CPUTime:       m2.DiskSpillSize,
		ExecTime:      m2.ExecTime,
		Nodes:         nodes,
		Stmt:          dm["Sql"].(string),
	}
}
