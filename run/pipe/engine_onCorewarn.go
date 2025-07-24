/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    engine_corewarn
 *@date    2025/7/2 13:23
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"strconv"
	"time"
)

// 白名单超过1个小时运行语句，发出提醒信息
func (w *Workers) handleOnCore(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2, schema []string) error {
	edtime, _ := strconv.Atoi(item.Time)

	// 小于1个小时的，驳回
	if edtime < 3600 {
		return nil
	}

	cid := fmt.Sprintf("%d_%s", 99, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}

	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnCoreWarn",
		Id:   item.Id,
	})

	body, sdata := InProcess(
		&util.InQue{
			Sign:       Singnel(99),
			App:        app,
			Fe:         fe,
			Item:       item,
			Logfile:    logfile,
			Edtime:     edtime,
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Action:     99,
			Connect:    db,
			Iceberg:    "",
		})

	util.Loggrs.Info(uid, "channel S.")
	w.lark <- body
	w.data <- sdata
	util.Loggrs.Info(uid, "channel D.")

	go Onkill(99, app, fe, item.Id)

	return nil
}
