/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendFullScan
 *@date    2024/9/2 10:47
 */

package pipe

import (
	"StarRocksQueris/run/explain"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (w *Workers) handleOnFscan(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2, schema []string) error {
	if util.ConnectNorm.SlowQueryFrontendFullscanNum == 0 {
		return nil
	}
	if item.Command != "Query" {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 4, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}

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

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnFscan",
		Id:   item.Id,
	})

	// 【获取内表副本分布情况，排序键分析】
	nt := time.Now()
	_, olap, err := explain.ReplicaDistribution(db, schema)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("副本分布分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// 【实用分区与空分区的获取】
	nt = time.Now()
	rangerMap := explain.IsPartitionMap(db, olap)
	util.Loggrs.Info(uid, fmt.Sprintf("分区数量统计 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【解析执行计划】
	nt = time.Now()
	olapscan, exfile, err := explain.ExplainQuery(db, item, rangerMap)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("查询计划分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))

	fullscanNum := util.ConnectNorm.SlowQueryFrontendFullscanNum
	if olapscan != nil {
		if olapscan.OlapCount < fullscanNum {
			return nil
		}
	}

	// 查询语句落文件
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	nature := "intercept"

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
				util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入查询队列（全表扫描+2亿数据量拦截）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Opinion:    "全表扫描数据量较大目前已经被拦截，需整改范围限制分区再提交，否侧将会继续触发拦截！",
						Sign:       Singnel(4),
						Nature:     nature,
						App:        app,
						Fe:         fe,
						Item:       item,
						Logfile:    logfile,
						Queris:     &qus,
						Larkcache:  LarkConnectionId,
						Emailcache: MailConnectionId,
						Action:     4,
						Olapscan:   olapscan,
						Connect:    db,
						Iceberg:    ice,
						Explog:     exfile,
					})

				go Onkill(4, app, fe, item.Id)
				w.lark <- body
				w.data <- sdata
				// 从这里开始，将IP地址信息进行落表
				w.clientChan(item)
				return nil
			}
		}
	}

	// end
	if olapscan == nil {
		return nil
	}
	if olapscan.OlapCount < fullscanNum {
		return nil
	}
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入普通进程（全表扫描+2亿数据量拦截）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Opinion:    "全表扫描数据量较大目前已经被拦截，需整改范围限制分区再提交，否侧将会继续触发拦截！",
			Sign:       Singnel(4),
			Nature:     nature,
			App:        app,
			Fe:         fe,
			Item:       item,
			Logfile:    logfile,
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Action:     4,
			Olapscan:   olapscan,
			Connect:    db,
			Iceberg:    ice,
			Explog:     exfile,
		})

	go Onkill(4, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(item)
	return nil
}
