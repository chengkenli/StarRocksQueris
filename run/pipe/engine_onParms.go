/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendHandleOnAvgs
 *@date    2024/8/21 18:05
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"time"
)

// handleOnSession 筛选每个查询是否存在异常参数
func (w *Workers) handleOnStatus(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2) error {
	if item.State != "ERR" {
		return nil
	}
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 0, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}
	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnStatus",
		Id:   item.Id,
	})
	// 查询语句落文件
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	// 分析查询语句与已经入库的语句相似百分比
	util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] %s", app, fe, item.Id, Singnel(0)))
	body, sdata := InProcess(
		&util.InQue{
			Opinion:    "提交的语句状态已经异常，但依旧挂载在进程中，进行清退规则！",
			Sign:       Singnel(0),
			Nature:     "清退",
			App:        app,
			Fe:         fe,
			Item:       item,
			Logfile:    logfile,
			Queryid:    nil,
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Action:     0,
			Connect:    db,
		})

	go Onkill(0, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(item)

	return nil
}
