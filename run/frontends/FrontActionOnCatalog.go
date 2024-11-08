/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnCatalog
 *@date    2024/10/15 18:13
 */

package fronends

import (
	"StarRocksQueris/run/explain"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (w *Workers) handleOnCatalog(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if util.ConnectNorm["slow_query_frontend_insert_catalog_scanrow"] == nil {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 7, item.Id)
	_, ok := FsCache.Get(cid)
	if ok {
		return nil
	}

	util.Loggrs.Info(fmt.Sprintf("%-10s %-6s #INSERT FROM CATALOG扫描数据量检查", fe, item.Id))

	// 解析语句，提取表名
	schema, err := SessionSchemaRegexp(item.Info)
	if err != nil {
		util.Loggrs.Error(err.Error())
	}

	if !strings.Contains(strings.Join(schema, ","), "hive.") {
		return nil
	}
	if !strings.Contains(strings.ToLower(item.Info), "insert") {
		return nil
	}
	if strings.Contains(strings.ToLower(item.Info), "where") {
		return nil
	}
	Yeah, _ := regexp.MatchString(`[=><]`, strings.ToLower(item.Info))
	if Yeah {
		return nil
	}
	catScanrow := int(util.ConnectNorm["slow_query_frontend_insert_catalog_scanrow"].(int32))

	var olapscan *util.OlapScanExplain
	for i := 0; i < 3; i++ {
		// 解析执行计划
		olapscan, err = explain.ExplainQuery(db, item, nil)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("#%d [done].查询计划分析 %s %v - %v", i, app, item.Id, olapscan))

		if olapscan == nil && queries == nil {
			return nil
		}

		if olapscan != nil {
			if olapscan.OlapCount == 0 {
				continue
			}
			if olapscan.OlapCount < catScanrow {
				return nil
			}
			break
		}
	}

	// 查询语句落文件
	logfile := fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	nature := "catalog - 从catalog通过insert方式写入的数据量过大，目前已经被拦截！请使用broker load的方式进行导入！"
	opinion := "catalog扫描数据量超过亿级 + INSERT TABLE FROM CATALOG "

	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> ", Singnel(7))
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       7,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         Singnel(7),
			})
	}()

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	queryid := TFIDF(item.Info)
	// 新逻辑，show processlist 与 队列绑定
	if queries != nil {
		for _, q := range queries {
			if q.ConnectionId == item.Id && q.User == item.User {

				if olapscan == nil {
					ScanRows, _ := strconv.Atoi(q.ScanRows)
					if ScanRows < catScanrow {
						continue
					}
				}

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
				util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入查询队列（参数拦截）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Opinion:  opinion,
						Sign:     Singnel(7),
						Nature:   nature,
						App:      app,
						Fe:       fe,
						Item:     item,
						Logfile:  logfile,
						Queryid:  queryid,
						Queris:   &qus,
						FsCache:  FsCache,
						EmCache:  EmCache,
						Action:   7,
						Olapscan: olapscan,
						Connect:  db,
						Iceberg:  ice,
					})

				go Onkill(7, app, fe, item.Id)
				w.lark <- body
				w.data <- sdata
				return nil
			}
		}
	}
	// end
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入普通进程（参数拦截）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Opinion:  opinion,
			Sign:     Singnel(7),
			Nature:   nature,
			App:      app,
			Fe:       fe,
			Item:     item,
			Logfile:  logfile,
			Queryid:  queryid,
			FsCache:  FsCache,
			EmCache:  EmCache,
			Action:   7,
			Schema:   schema,
			Olapscan: olapscan,
			Connect:  db,
			Iceberg:  ice,
		})

	go Onkill(7, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	return nil
}
