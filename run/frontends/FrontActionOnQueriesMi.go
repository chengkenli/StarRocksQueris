/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnQueries
 *@date    2024/9/6 17:37
 */

package fronends

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
	"time"
)

func (w *Workers) handleOnQueriesMi(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if util.ConnectNorm["slow_query_frontend_scanrows"] == nil {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 6, item.Id)
	_, ok := FsCache.Get(cid)
	if ok {
		return nil
	}

	util.Loggrs.Info(fmt.Sprintf("%-10s %-6s #百亿级别扫描行数检查", fe, item.Id))

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	nature := "intercept"
	if queries != nil {
		for _, q := range queries {
			if Int64(q.ScanRows) < util.ConnectNorm["slow_query_frontend_scanrows"].(int64) {
				continue
			}
			if q.ConnectionId != item.Id {
				continue
			}
			// 解析语句，提取表名
			schema, err := SessionSchemaRegexp(item.Info)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
			util.Loggrs.Info(fmt.Sprintf("%v === %s", q.ConnectionId, item.Id))
			qus := QuerisA(db, app, fe,
				&util.Querisign{
					StartTime:     q.StartTime,
					QueryId:       q.QueryId,
					ConnectionId:  q.ConnectionId,
					Database:      q.Database,
					User:          q.User,
					ScanBytes:     q.ScanBytes,
					ScanRows:      q.ScanRows,
					MemoryUsage:   q.MemoryUsage,
					DiskSpillSize: q.DiskSpillSize,
					CPUTime:       q.CPUTime,
					ExecTime:      q.ExecTime,
					Warehouse:     q.Warehouse,
				})

			body, sdata := InQueris(
				&util.InQue{
					Opinion: "扫描行数已经达到百亿级别，需整改限制分区缩小范围，避免继续触发拦截！",
					Sign:    Singnel(6),
					Nature:  nature,
					App:     app,
					Schema:  schema,
					Fe:      fe,
					Queris:  &qus,
					FsCache: FsCache,
					EmCache: EmCache,
					Item:    item,
					Action:  6,
					Logfile: fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano()),
					Iceberg: ice,
				})

			// 向监控汇报数据
			go func() {
				util.Loggrs.Info("ga -> ", Singnel(6))
				FrontGrafana(
					&util.Grafana{
						App:          app,
						Action:       6,
						ConnectionId: item.Id,
						User:         item.User,
						Sign:         Singnel(6),
					})
			}()
			go Onkill(6, app, fe, item.Id)
			w.lark <- body
			w.data <- sdata
			return nil
		}
	}
	return nil
}
