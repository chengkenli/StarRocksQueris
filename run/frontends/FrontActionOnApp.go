/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnSession
 *@date    2024/8/21 18:03
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

// handleOnSessionApp 筛选每个查询是否满足慢查询条件 (专门为了报表集群，svccnrpths用户执行的逻辑)
func handleOnSessionApp(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) (*util.Larkbodys, *util.SchemaData) {
	// 缓存中拿到session id，如果存在，那么结束
	edtime, _ := strconv.Atoi(item.Time)
	var an int
	if edtime >= util.SlowQueryDangerKillTime {
		an = 3
	} else {
		an = 2
	}

	cid := fmt.Sprintf("%d_%s", an, item.Id)
	util.Loggrs.Info(fmt.Sprintf("***************获取缓存:%s", cid))

	_, ok := FsCache.Get(cid)
	if ok {
		return nil, nil
	}

	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	var schema, rd, olap []string
	go func() {
		defer wg.Done()
		// 解析语句，提取表名
		util.Loggrs.Info(fmt.Sprintf("[star].查询表名提取 %s %v", app, item.Id))
		schema, err = SessionSchemaRegexp(item.Info)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("[done].查询表名提取 %s %v", app, item.Id))
		// 获取内表副本分布情况，排序键分析
		util.Loggrs.Info(fmt.Sprintf("[star].副本分布分析 %s %v", app, item.Id))
		rd, olap, err = explain.ReplicaDistribution(db, schema)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("[done].副本分布分析 %s %v", app, item.Id))
	}()
	wg.Wait()

	wg.Add(4)
	var olapscan *util.OlapScanExplain
	go func() {
		defer wg.Done()
		// 实用分区与空分区的获取
		rangerMap := explain.IsPartitionMap(db, olap)
		util.Loggrs.Info(fmt.Sprintf("[done].分区数量统计 %s %v", app, item.Id))
		// 解析执行计划
		util.Loggrs.Info(fmt.Sprintf("[star].查询计划分析 %s %v", app, item.Id))
		olapscan, err = explain.ExplainQuery(db, item, rangerMap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("[done].查询计划分析 %s %v", app, item.Id))
	}()

	var sortkey []*util.SchemaSortKey
	go func() {
		defer wg.Done()
		// 排序键分析
		util.Loggrs.Info(fmt.Sprintf("[star].前排序键分析 %s %v", app, item.Id))
		sortkey, err = explain.ScanSchemaSortKey(db, olap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("[done].前排序键分析 %s %v", app, item.Id))
	}()

	var buckets []string
	var normal bool
	go func() {
		defer wg.Done()
		// 分桶倾斜分析
		util.Loggrs.Info(fmt.Sprintf("[star].分桶倾斜分析 %s %v", app, item.Id))
		buckets, normal, err = explain.GetBuckets(app, olap)
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		util.Loggrs.Info(fmt.Sprintf("[done].分桶倾斜分析 %s %v", app, item.Id))
	}()

	var queryid []string
	go func() {
		defer wg.Done()
		// 分析查询语句与已经入库的语句相似百分比
		util.Loggrs.Info(fmt.Sprintf("[chck].余弦相似分析 %s %v", app, item.Id))
		queryid = TFIDF(item.Info)
	}()
	wg.Wait()

	// 查询语句落文件
	util.Loggrs.Info(fmt.Sprintf("[star].查询保存文件 %s %v", app, item.Id))
	logfile := fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	var tbs []string
	for _, tbname := range schema {
		table := explain.ExOlapOrView(db, tbname)
		if tbname == table {
			tbs = append(tbs, tbname)
		} else {
			tbs = append(tbs, fmt.Sprintf("%s(%s)", tbname, table))
		}
	}
	util.Loggrs.Info(fmt.Sprintf("[done].裁判内表视图 %s %v", app, item.Id))

	var action int
	var sign string
	if edtime >= util.SlowQueryDangerKillTime {
		sign = "慢查询查杀"
		action = 3
		go Onkill(3, app, fe, item.Id)
	} else {
		action = 2
		sign = "慢查询告警"
	}

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> 核心app")
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       action,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         sign,
			})
	}()
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
				util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入查询队列（核心报表）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Sign:     "核心报表",
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
				return body, sdata
			}
		}
	}
	// end
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] 进入普通进程（核心报表）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Sign:     "核心报表",
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
	return body, sdata
}
