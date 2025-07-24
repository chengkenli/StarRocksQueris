/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendHandleOnSession
 *@date    2024/8/21 18:03
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

// handleOnSessionApp 筛选每个查询是否满足慢查询条件 (专门为了报表集群，svccnrpths用户执行的逻辑)
func (w *Workers) handleOnSessionApp(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	// 缓存中拿到session id，如果存在，那么结束
	edtime, _ := strconv.Atoi(item.Time)
	var an int
	if edtime >= util.SlowQueryDangerKillTime {
		an = 3
	} else {
		an = 2
	}
	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "SessionApp",
		Id:   item.Id,
	})

	cid := fmt.Sprintf("%d_%s", an, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}
	// 查询语句落文件
	util.Loggrs.Info(uid, fmt.Sprintf("[star].查询保存文件 %s %v", app, item.Id))
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	var action int
	if edtime >= util.SlowQueryDangerKillTime {
		action = 3
	} else {
		action = 2
	}

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}
	resultData := emoExplain(db, app, item)

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
				util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入查询队列（核心报表）...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Sign:       "生产报表5min超时查杀",
						App:        app,
						Fe:         fe,
						Tbs:        resultData.OlapTables,
						Rd:         resultData.SortReport,
						Item:       item,
						Olapscan:   resultData.OlapScan,
						Sortkey:    resultData.SortKeys,
						Buckets:    resultData.BucketResult,
						Logfile:    logfile,
						Normal:     resultData.BucketType,
						Queryid:    resultData.QueryIds,
						Edtime:     edtime,
						Schema:     resultData.OlapSchema,
						Queris:     &qus,
						Larkcache:  LarkConnectionId,
						Emailcache: MailConnectionId,
						Action:     action,
						Connect:    db,
						Iceberg:    ice,
						Explog:     resultData.ExplainFile,
					})

				go Onkill(action, app, fe, item.Id)

				w.lark <- body
				w.data <- sdata
				// 从这里开始，将IP地址信息进行落表
				w.clientChan(item)
				return nil
			}
		}
	}
	// end
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入普通进程（核心报表）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Sign:       "生产报表5min超时查杀",
			App:        app,
			Fe:         fe,
			Tbs:        resultData.OlapTables,
			Rd:         resultData.SortReport,
			Item:       item,
			Olapscan:   resultData.OlapScan,
			Sortkey:    resultData.SortKeys,
			Buckets:    resultData.BucketResult,
			Logfile:    logfile,
			Normal:     resultData.BucketType,
			Queryid:    resultData.QueryIds,
			Edtime:     edtime,
			Schema:     resultData.OlapSchema,
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Action:     action,
			Connect:    db,
			Iceberg:    ice,
			Explog:     resultData.ExplainFile,
		})

	go Onkill(action, app, fe, item.Id)

	w.lark <- body
	w.data <- sdata
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(item)
	return nil
}
