/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendHandleOnQueries
 *@date    2024/9/6 17:37
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"strings"
	"time"
)

func (w *Workers) handleOnQueriesMi(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2, schema []string) error {

	if util.ConnectNorm.SlowQueryFrontendScanrows <= 0 {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 6, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}

	// iceberg 重要提醒
	var ice string
	if strings.Contains(item.Info, "iceberg.") {
		ice = "语句中包含iceberg catalog表，请务必保证iceberg表中的deletefile不能太多，否则请先合并文件！"
	}
	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnQueriesMi",
		Id:   item.Id,
	})
	nature := "intercept"
	if queries != nil {
		for _, q := range queries {
			if Int64(q.ScanRows) < util.ConnectNorm.SlowQueryFrontendScanrows {
				continue
			}
			if q.ConnectionId != item.Id {
				continue
			}
			util.Loggrs.Info(uid, fmt.Sprintf("%v === %s", q.ConnectionId, item.Id))
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
					Opinion:    "扫描行数已经达到百亿级别，需整改限制分区缩小范围，避免继续触发拦截！",
					Sign:       Singnel(6),
					Nature:     nature,
					App:        app,
					Schema:     schema,
					Fe:         fe,
					Queris:     &qus,
					Larkcache:  LarkConnectionId,
					Emailcache: MailConnectionId,
					Item:       item,
					Action:     6,
					Logfile:    fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano()),
					Iceberg:    ice,
					Explog:     "",
				})

			go Onkill(6, app, fe, item.Id)
			w.lark <- body
			w.data <- sdata
			// 从这里开始，将IP地址信息进行落表
			w.clientChan(item)
			return nil
		}
	}
	return nil
}
