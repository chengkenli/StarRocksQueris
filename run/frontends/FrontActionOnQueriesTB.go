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

// TB级别的查询扫描的字节数
func (w *Workers) handleOnQueriesTB(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if util.ConnectNorm.SlowQueryFrontendScanbytes <= 0 {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 5, item.Id)
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
			if !strings.Contains(strings.ToLower(q.ScanBytes), " tb") {
				continue
			}
			mu := strings.Split(q.ScanBytes, " ")
			if len(mu) < 2 {
				continue
			}
			scanBytes, _ := strconv.ParseFloat(mu[0], 64)
			if scanBytes < float64(util.ConnectNorm.SlowQueryFrontendScanbytes) {
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
					Opinion: "扫描消耗字节已超5TB+，需整改限制分区缩小查询范围，避免继续触发拦截！",
					Sign:    Singnel(5),
					Nature:  nature,
					App:     app,
					Schema:  schema,
					Fe:      fe,
					Queris:  &qus,
					FsCache: FsCache,
					EmCache: EmCache,
					Item:    item,
					Action:  5,
					Logfile: fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano()),
					Iceberg: ice,
				})

			// 向监控汇报数据
			go func() {
				util.Loggrs.Info("ga -> ", Singnel(5))
				FrontGrafana(
					&util.Grafana{
						App:          app,
						Action:       5,
						ConnectionId: item.Id,
						User:         item.User,
						Sign:         Singnel(5),
					})
			}()

			go Onkill(5, app, fe, item.Id)
			w.lark <- body
			w.data <- sdata
			return nil
		}
	}
	return nil
}
