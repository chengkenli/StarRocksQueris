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
	"strconv"
	"strings"
	"time"
)

// GB级别查询消耗内存
func (w *Workers) handleOnQueriesGB(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if util.ConnectNorm["slow_query_frontend_memoryusage"] == nil {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 8, item.Id)
	_, ok := FsCache.Get(cid)
	if ok {
		return nil
	}

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	nature := "intercept"
	if queries != nil {
		for _, q := range queries {

			if q.ConnectionId != item.Id {
				continue
			}
			if !strings.Contains(strings.ToLower(q.MemoryUsage), " gb") {
				continue
			}
			mu := strings.Split(q.MemoryUsage, " ")
			if len(mu) < 2 {
				continue
			}
			memoryUsage, _ := strconv.Atoi(mu[0])
			maxMemoryUsage := int(util.ConnectNorm["slow_query_frontend_memoryusage"].(int64))
			if memoryUsage < maxMemoryUsage {
				continue
			}
			util.Loggrs.Info(fmt.Sprintf("%-10s %-6s #%dGB+级别查询消耗内存检查", fe, item.Id, maxMemoryUsage))
			//------------------------------- 开始计算这个语句中每个BE的消耗内存各是多少
			nodes := UriCQHIP(app, fe, q.QueryId)
			if len(nodes) == 0 {
				continue
			}
			//------------------------------- end

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
					MemoryUsage:   fmt.Sprintf("%s(%s)", q.MemoryUsage, strings.Join(nodes, ",")),
					DiskSpillSize: q.DiskSpillSize,
					CPUTime:       q.CPUTime,
					ExecTime:      q.ExecTime,
					Warehouse:     q.Warehouse,
				})

			body, sdata := InQueris(
				&util.InQue{
					Opinion: fmt.Sprintf("查询消耗单节点内存已达%dGB+，消耗节点内存90%%，需整改限制分区缩小查询范围或修改相关逻辑，避免继续触发拦截！", maxMemoryUsage),
					Sign:    Singnel(8),
					Nature:  nature,
					App:     app,
					Schema:  schema,
					Fe:      fe,
					Queris:  &qus,
					FsCache: FsCache,
					EmCache: EmCache,
					Item:    item,
					Action:  8,
					Logfile: fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano()),
					Iceberg: ice,
				})

			// 向监控汇报数据
			go func() {
				util.Loggrs.Info(fmt.Sprintf("ga -> %s", Singnel(8)))
				FrontGrafana(
					&util.Grafana{
						App:          app,
						Action:       8,
						ConnectionId: item.Id,
						User:         item.User,
						Sign:         Singnel(8),
					})
			}()

			go Onkill(8, app, fe, item.Id)

			w.lark <- body
			w.data <- sdata

			return nil
		}
	}
	return nil
}
