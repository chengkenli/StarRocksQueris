/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnAvgs
 *@date    2024/8/21 18:05
 */

package fronends

import (
	"StarRocksQueris/util"
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
	_, ok := FsCache.Get(fmt.Sprintf("%d_%s", 0, item.Id))
	if ok {
		return nil
	}
	// 查询语句落文件
	logfile := fmt.Sprintf("%s/%s_%s_%s_%d.sql", util.LogPath, item.User, item.Id, item.Time, time.Now().UnixNano())
	// 分析查询语句与已经入库的语句相似百分比
	// 向监控汇报数据
	go func() {
		util.Loggrs.Info("ga -> ", Singnel(0))
		FrontGrafana(
			&util.Grafana{
				App:          app,
				Action:       0,
				ConnectionId: item.Id,
				User:         item.User,
				Sign:         "参数拦截",
			})
	}()
	util.Loggrs.Info(fmt.Sprintf(">>>>>>>>>>[%s][%s][%s] %s", app, fe, item.Id, Singnel(0)))
	body, sdata := InProcess(
		&util.InQue{
			Opinion: "提交的语句状态已经异常，但依旧挂载在进程中，进行清退规则！",
			Sign:    Singnel(0),
			Nature:  "清退",
			App:     app,
			Fe:      fe,
			Item:    item,
			Logfile: logfile,
			Queryid: nil,
			FsCache: FsCache,
			EmCache: EmCache,
			Action:  0,
			Connect: db,
		})

	go Onkill(0, app, fe, item.Id)
	w.lark <- body
	w.data <- sdata
	return nil
}
