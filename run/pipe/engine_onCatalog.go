/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendHandleOnCatalog
 *@date    2024/10/15 18:13
 */

package pipe

import (
	"StarRocksQueris/run/explain"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (w *Workers) handleOnCatalog(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2, schema []string) error {
	if util.ConnectNorm.SlowQueryFrontendInsertCatalogScanrow <= 0 {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 7, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
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
	catScanrow := util.ConnectNorm.SlowQueryFrontendInsertCatalogScanrow

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnCatalog",
		Id:   item.Id,
	})
	// 【获取内表副本分布情况，排序键分析】
	nt := time.Now()
	_, olap, err := explain.ReplicaDistribution(db, schema)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("副本分布分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
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
	// ------------------------------------------
	// 【分析查询语句与已经入库的语句相似百分比】
	nt = time.Now()
	util.Loggrs.Info(uid, fmt.Sprintf("余弦相似分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	queryid := TFIDF(item.Info)
	// ------------------------------------------
	// 如果执行计划和队列的都是都是空的，那么返回
	if olapscan == nil && queries == nil {
		return nil
	}

	// 从执行计划里面拿到数据量
	util.Loggrs.Info(uid, fmt.Sprintf("%s %s olapscan.OlapCount >: 从执行计划里面拿到的数据量是：%v", app, item.Id, olapscan))

	// 查询语句落文件
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	nature := "catalog - 从catalog通过insert方式写入的数据量过大，目前已经被拦截！请使用broker load的方式进行导入！"
	opinion := "catalog扫描数据量超过亿级 + INSERT TABLE FROM CATALOG "

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}

	if queries != nil {
		// 新逻辑，show processlist 与 队列绑定
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
				util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入查询队列（参数拦截）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Opinion:    opinion,
						Sign:       Singnel(7),
						Nature:     nature,
						App:        app,
						Fe:         fe,
						Item:       item,
						Logfile:    logfile,
						Queryid:    queryid,
						Queris:     &qus,
						Larkcache:  LarkConnectionId,
						Emailcache: MailConnectionId,
						Action:     7,
						Olapscan:   olapscan,
						Connect:    db,
						Iceberg:    ice,
						Explog:     exfile,
					})

				go Onkill(7, app, fe, item.Id)
				w.lark <- body
				w.data <- sdata
				// 从这里开始，将IP地址信息进行落表
				w.clientChan(item)
				return nil
			}
		}
	}

	if olapscan == nil {
		return nil
	}
	// 再加一层机制
	marshal, _ := json.Marshal(olapscan)
	util.Loggrs.Warn(uid, fmt.Sprintf("执行计划的行数?%v", string(marshal)))
	if olapscan.OlapCount < catScanrow {
		return nil
	}

	if olapscan.OlapCount > catScanrow {
		// end
		// 当吸收队列失败，那么进行普通告警
		util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入普通进程（参数拦截）...", app, fe, item.Id))
		body, sdata := InProcess(
			&util.InQue{
				Opinion:    opinion,
				Sign:       Singnel(7),
				Nature:     nature,
				App:        app,
				Fe:         fe,
				Item:       item,
				Logfile:    logfile,
				Queryid:    queryid,
				Larkcache:  LarkConnectionId,
				Emailcache: MailConnectionId,
				Action:     7,
				Schema:     schema,
				Olapscan:   olapscan,
				Connect:    db,
				Iceberg:    ice,
				Explog:     exfile,
			})
		go Onkill(7, app, fe, item.Id)
		w.lark <- body
		w.data <- sdata
		// 从这里开始，将IP地址信息进行落表
		w.clientChan(item)
	}

	return nil
}
