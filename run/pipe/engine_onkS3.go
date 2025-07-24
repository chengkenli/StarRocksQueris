/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    Run_Front_OnkS3
 *@date    2025/4/22 18:11
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

func (w *Workers) handleOnkillS3(db *gorm.DB, app, fe string, queries util.Queris, item *util.Process2, schema []string) error {
	// 缓存中拿到session id，如果存在，那么结束
	cid := fmt.Sprintf("%d_%s", 9, item.Id)
	_, ok := LarkConnectionId.Get(cid)
	if ok {
		return nil
	}

	if !strings.Contains(item.Info, "access_key") {
		return nil
	}

	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   fe,
		Mode: "OnkillS3",
		Id:   item.Id,
	})
	// 【分析查询语句与已经入库的语句相似百分比】
	nt := time.Now()
	util.Loggrs.Info(uid, fmt.Sprintf("余弦相似分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	queryid := TFIDF(item.Info)
	nature := "intercept"

	// 当吸收队列失败，那么进行普通告警
	util.Loggrs.Info(uid, fmt.Sprintf("[%s][%s][%s] 进入普通进程（S3协议拦截）...", app, fe, item.Id))
	body, sdata := InProcess(
		&util.InQue{
			Opinion:    "提交的语句发现存在S3协议，请使用kerberos方式进行访问，避免再次触发拦截！",
			Sign:       Singnel(9),
			Nature:     nature,
			App:        app,
			Fe:         fe,
			Item:       item,
			Logfile:    logfile,
			Queryid:    queryid,
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Avgs:       []string{"access_key", "secret_key"},
			Action:     9,
			Connect:    db,
		})

	go Onkill(9, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(item)
	return nil
}
