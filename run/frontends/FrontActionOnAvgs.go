/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnAvgs
 *@date    2024/8/21 18:05
 */

package fronends

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// handleOnSession 筛选每个查询是否存在异常参数
func (w *Workers) handleOnAvgs(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {

	if len(util.ConnectNorm.SlowQueryFrontendAvgs) == 0 {
		return nil
	}
	if item.Command != "Query" {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 1, item.Id)
	_, ok := FsCache.Get(cid)
	if ok {
		return nil
	}

	// 查询语句落文件
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	matches := regexp.MustCompile(`([^\s=]+)\s*=\s*([^\s;]+)`).FindAllStringSubmatch(item.Info, -1)
	if matches == nil {
		return nil
	}
	var str []string
	for _, match := range matches {
		if strings.Contains(strings.Join(match, ","), ".") ||
			strings.Contains(strings.Join(match, ","), ">") ||
			strings.Contains(strings.Join(match, ","), "<") ||
			strings.Contains(strings.Join(match, ","), "(") ||
			strings.Contains(strings.Join(match, ","), ")") ||
			strings.Contains(strings.Join(match, ","), "'") ||
			strings.Contains(strings.Join(match, ","), "`") {
			continue
		}
		if len(match) < 3 {
			continue
		}

		frontendAvgs := util.ConnectNorm.SlowQueryFrontendAvgs

		for _, sign := range strings.Split(frontendAvgs, ",") {
			voe := strings.Split(sign, "=")
			if len(voe) < 2 {
				continue
			}
			agkey := voe[0]
			agval := voe[1]
			if tools.StringInSlice(agkey, match) {
				util.Loggrs.Info(fmt.Sprintf("[chck].命中异常参数 %s %v 1.[%s],2.[%s]", app, item.Id, match[1], match[2]))
				// query_mem_limit,load_mem_limit,exec_mem_limit
				if match[1] == agkey {
					value, _ := strconv.ParseInt(match[2], 10, 64)
					val, _ := strconv.ParseInt(agval, 10, 64)
					if value >= val {
						str = append(str, match[0])
					}
				}
			}
		}
	}
	if len(str) == 0 {
		return nil
	}

	// 分析查询语句与已经入库的语句相似百分比
	util.Loggrs.Info(fmt.Sprintf("[chck].余弦相似分析 %s %v", app, item.Id))
	queryid := TFIDF(item.Info)

	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> ", Singnel(1))
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       1,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         Singnel(1),
			})
	}()

	nature := "intercept"
	// 新逻辑，show processlist 与 队列绑定
	if queries != nil {
		for _, q := range queries {
			if q.ConnectionId == item.Id && q.User == item.User {
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
						Opinion: "提交的语句存在异常参数，请删除相关参数或调低阈值，避免继续触发拦截！",
						Sign:    Singnel(1),
						Nature:  nature,
						App:     app,
						Fe:      fe,
						Item:    item,
						Logfile: logfile,
						Queryid: queryid,
						Queris:  &qus,
						FsCache: FsCache,
						EmCache: EmCache,
						Avgs:    str,
						Action:  1,
						Connect: db,
					})

				go Onkill(1, app, fe, item.Id)

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
			Opinion: "提交的语句存在异常参数，请删除相关参数或调低阈值，否侧将会继续触发拦截！",
			Sign:    Singnel(1),
			Nature:  nature,
			App:     app,
			Fe:      fe,
			Item:    item,
			Logfile: logfile,
			Queryid: queryid,
			FsCache: FsCache,
			EmCache: EmCache,
			Avgs:    str,
			Action:  1,
			Connect: db,
		})

	go Onkill(1, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	return nil
}
