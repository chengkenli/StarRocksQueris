/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendHandleOnSession
 *@date    2024/8/21 18:02
 */

package pipe

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"strconv"
	"strings"
	"time"
)

// handleOnSession 筛选每个查询是否满足慢查询条件
func (w *Workers) handleOnGlobal(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if item.Command != "Query" {
		return nil
	}
	edtime, _ := strconv.Atoi(item.Time)
	// 报告集群，svccnrpths处理的逻辑, 判断集群，用户名，还有超时时间
	if app == "sr-app" && tools.StringInSlice(item.User, strings.Split(util.SlowQueryDangerUser, ",")) && edtime >= util.SlowQueryDangerKillTime {
		err := w.handleOnSessionApp(db, app, fe, queries, item)
		if err != nil {
			util.Loggrs.Error(err.Error())
			return err
		}
		return nil
	}
	if edtime < util.ConnectNorm.SlowQueryTime {
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
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}
	// 新逻辑，show processlist 与 队列绑定
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnGlobal",
		Id:   item.Id,
	})
	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}
	resultData := emoExplain(db, app, item)
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
				util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入查询队列...", app, fe, item.Id))
				body, sdata := InQueris(
					&util.InQue{
						Sign:       Singnel(action),
						App:        app,
						Fe:         fe,
						Tbs:        resultData.OlapView,
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

				util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 完成查询队列...", app, fe, item.Id))

				util.Loggrs.Info(uid, "channel S.")
				w.lark <- body
				w.data <- sdata
				// 从这里开始，将IP地址信息进行落表
				w.clientChan(item)
				util.Loggrs.Info(uid, "channel D.")
				return nil
			}
		}
	}
	// end
	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入普通进程...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Sign:       Singnel(action),
			App:        app,
			Fe:         fe,
			Tbs:        resultData.OlapView,
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

	util.Loggrs.Info(uid, "channel S.")
	w.lark <- body
	w.data <- sdata
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(item)
	util.Loggrs.Info(uid, "channel D.")

	return nil
}
