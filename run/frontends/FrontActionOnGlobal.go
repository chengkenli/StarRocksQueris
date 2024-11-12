/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnSession
 *@date    2024/8/21 18:02
 */

package fronends

import (
	"StarRocksQueris/run/explain"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"sync"
	"time"
)

// handleOnSession 筛选每个查询是否满足慢查询条件
func (w *Workers) handleOnGlobal(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if item.Command != "Query" {
		return nil
	}
	edtime, _ := strconv.Atoi(item.Time)
	// 报告集群，svccnrpths处理的逻辑, 判断集群，用户名，还有超时时间
	//if app == "sr-app" && tools.StringInSlice(item.User, strings.Split(util.SlowQueryDangerUser, ",")) && edtime >= util.SlowQueryDangerKillTime {
	//	body, sdata := handleOnSessionApp(db, app, fe, queries, item)
	//	w.lark <- body
	//	w.data <- sdata
	//	return nil
	//}
	if edtime < int(util.ConnectNorm.SlowQueryTime) {
		return nil
	}

	// 缓存中拿到session id，如果存在，那么结束
	var action int
	if edtime >= util.ConnectNorm.SlowQueryKtime {
		action = 3
	} else {
		action = 2
	}
	cid := fmt.Sprintf("%d_%s", action, item.Id)
	_, ok := FsCache.Get(cid)
	if ok {
		return nil
	}

	var err error
	var schema, rd, olap []string
	var wg sync.WaitGroup
	//########################################################
	wg.Add(6)
	go func() {
		defer wg.Done()
		nt := time.Now()
		// 解析语句，提取表名
		schema, err = SessionSchemaRegexp(item.Info)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("查询表名提取 %s %v", item.Id, time.Now().Sub(nt).String()))
		// 获取内表副本分布情况，排序键分析
		nt = time.Now()
		rd, olap, err = explain.ReplicaDistribution(db, schema)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("副本分布分析 %s %v", item.Id, time.Now().Sub(nt).String()))
	}()
	//########################################################
	var olapscan *util.OlapScanExplain
	go func() {
		defer wg.Done()

		// 实用分区与空分区的获取
		nt := time.Now()
		rangerMap := explain.IsPartitionMap(db, olap)
		util.Loggrs.Info(fmt.Sprintf("分区数量统计 %s %v", item.Id, time.Now().Sub(nt).String()))
		// 解析执行计划
		nt = time.Now()
		olapscan, err = explain.ExplainQuery(db, item, rangerMap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("查询计划分析 %s %v", item.Id, time.Now().Sub(nt).String()))
	}()
	//########################################################
	var sortkey []*util.SchemaSortKey
	go func() {
		defer wg.Done()

		// 排序键分析
		nt := time.Now()
		sortkey, err = explain.ScanSchemaSortKey(db, olap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("前排序键分析 %s %v", item.Id, time.Now().Sub(nt).String()))
	}()
	//########################################################
	var buckets []string
	var normal bool
	go func() {
		defer wg.Done()

		// 分桶倾斜分析
		nt := time.Now()
		buckets, normal, err = explain.GetBuckets(app, olap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("分桶倾斜分析 %s %v", item.Id, time.Now().Sub(nt).String()))
	}()
	//########################################################
	var queryid []string
	go func() {
		defer wg.Done()

		nt := time.Now()
		// 分析查询语句与已经入库的语句相似百分比
		util.Loggrs.Info(fmt.Sprintf("余弦相似分析 %s %v", item.Id, time.Now().Sub(nt).String()))
		queryid = TFIDF(item.Info)
	}()
	//########################################################
	// 查询语句落文件
	var tbs []string
	go func() {
		defer wg.Done()

		nt := time.Now()
		for _, tbname := range schema {
			table := explain.ExOlapOrView(db, tbname)
			if tbname == table {
				tbs = append(tbs, tbname)
			} else {
				tbs = append(tbs, fmt.Sprintf("%s(%s)", tbname, table))
			}
		}
		util.Loggrs.Info(fmt.Sprintf("裁判内表视图 %s %v", item.Id, time.Now().Sub(nt).String()))
	}()
	wg.Wait()
	//########################################################
	// 新逻辑，show processlist 与 队列绑定
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> 全局", Singnel(action))
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       action,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         Singnel(action),
			})
	}()

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

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
				util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入查询队列...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Sign:     Singnel(action),
						App:      app,
						Fe:       fe,
						Tbs:      tbs,
						Rd:       rd,
						Item:     item,
						Olapscan: olapscan,
						Sortkey:  sortkey,
						Buckets:  buckets,
						Logfile:  logfile,
						Normal:   normal,
						Queryid:  queryid,
						Edtime:   edtime,
						Schema:   schema,
						Queris:   &qus,
						FsCache:  FsCache,
						EmCache:  EmCache,
						Action:   action,
						Connect:  db,
						Iceberg:  ice,
					})

				go Onkill(action, app, fe, item.Id)

				util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 完成查询队列...", app, fe, item.Id))

				util.Loggrs.Info("channel S.")
				w.lark <- body
				w.data <- sdata
				util.Loggrs.Info("channel D.")
				return nil
			}
		}
	}
	// end
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入普通进程...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Sign:     Singnel(action),
			App:      app,
			Fe:       fe,
			Tbs:      tbs,
			Rd:       rd,
			Item:     item,
			Olapscan: olapscan,
			Sortkey:  sortkey,
			Buckets:  buckets,
			Logfile:  logfile,
			Normal:   normal,
			Queryid:  queryid,
			Edtime:   edtime,
			Schema:   schema,
			FsCache:  FsCache,
			EmCache:  EmCache,
			Action:   action,
			Connect:  db,
			Iceberg:  ice,
		})

	go Onkill(action, app, fe, item.Id)

	util.Loggrs.Info("channel S.")
	w.lark <- body
	w.data <- sdata
	util.Loggrs.Info("channel D.")

	return nil
}
