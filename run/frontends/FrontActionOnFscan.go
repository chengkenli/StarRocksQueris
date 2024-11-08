/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendFullScan
 *@date    2024/9/2 10:47
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

func (w *Workers) handleOnFscan(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if util.ConnectNorm["slow_query_frontend_fullscan_num"] == nil {
		return nil
	}
	if item.Command != "Query" {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	_, ok := FsCache.Get(fmt.Sprintf("%d_%s", 4, item.Id))
	if ok {
		return nil
	}

	util.Loggrs.Info(fmt.Sprintf("%-10s %-6s #全表扫描亿级别数据量检查", fe, item.Id))

	Ok := regexp.MustCompile(`\bselect\s+\*\s+from\s+[a-zA-Z0-9.\_]+\b`).MatchString(strings.ToLower(item.Info))
	Yeah, _ := regexp.MatchString(`[=><]`, strings.ToLower(item.Info))
	if !Ok {
		return nil
	}
	if Yeah {
		return nil
	}
	if strings.Contains(strings.ToLower(item.Info), "insert") {
		return nil
	}
	// 解析执行计划
	olapscan, err := explain.ExplainQuery(db, item, nil)
	if err != nil {
		util.Loggrs.Error(err.Error())
	}
	if olapscan == nil && queries == nil {
		return nil
	}

	fullscanNum := int(util.ConnectNorm["slow_query_frontend_fullscan_num"].(int64))
	if olapscan != nil {
		if olapscan.OlapCount < fullscanNum {
			return nil
		}
	}

	// 查询语句落文件
	logfile := fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	nature := "intercept"

	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> ", Singnel(4))
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       4,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         Singnel(4),
			})
	}()

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	// 新逻辑，show processlist 与 队列绑定
	if queries != nil {
		for _, q := range queries {
			if q.ConnectionId == item.Id && q.User == item.User {

				if olapscan == nil {
					ScanRows, _ := strconv.Atoi(q.ScanRows)
					if ScanRows < fullscanNum {
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
				util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入查询队列（全表扫描+2亿数据量拦截）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Opinion:  "全表扫描数据量较大目前已经被拦截，需整改范围限制分区再提交，否侧将会继续触发拦截！",
						Sign:     Singnel(4),
						Nature:   nature,
						App:      app,
						Fe:       fe,
						Item:     item,
						Logfile:  logfile,
						Queris:   &qus,
						FsCache:  FsCache,
						EmCache:  EmCache,
						Action:   4,
						Olapscan: olapscan,
						Connect:  db,
						Iceberg:  ice,
					})

				go Onkill(4, app, fe, item.Id)
				w.lark <- body
				w.data <- sdata
				return nil
			}
		}
	}

	// end
	util.Loggrs.Info(olapscan)
	if olapscan.OlapCount < fullscanNum {
		return nil
	}
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入普通进程（全表扫描+2亿数据量拦截）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Opinion:  "全表扫描数据量较大目前已经被拦截，需整改范围限制分区再提交，否侧将会继续触发拦截！",
			Sign:     Singnel(4),
			Nature:   nature,
			App:      app,
			Fe:       fe,
			Item:     item,
			Logfile:  logfile,
			FsCache:  FsCache,
			EmCache:  EmCache,
			Action:   4,
			Olapscan: olapscan,
			Connect:  db,
			Iceberg:  ice,
		})

	go Onkill(4, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	return nil
}
