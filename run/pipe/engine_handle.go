/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_mhandle
 *@date    2024/11/6 14:52
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"time"
)

// 处理每个拦截模式
func (w *Workers) emomhandle(c *handle, p ...interface{}) {
	uid := xid.Xid(&xid.Uid{
		App:  c.App,
		Fe:   c.Fe,
		Mode: "handle",
		Id:   c.Item.Id,
	})
	// 统一处理
	nt := time.Now()
	schema, _ := SessionSchemaRegexp(c.Item.Info)
	util.Loggrs.Info(uid, fmt.Sprintf("查询表名提取 %s %s %v", c.App, c.Item.Id, time.Now().Sub(nt).String()))
	// 从这里开始，将IP地址信息进行落表
	w.clientChan(c.Item)
	// 从这里开始，将IP地址信息进行落表
	for _, i2 := range p {
		switch i2.(int) {
		case 0:
			// ===============[On]====================
			// 进程中语句已经异常中断，进行清退
			err := w.handleOnStatus(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 1:
			// ===============[On]====================
			//检查异常参数
			// 解析一系列操作【每个语句只做一次】
			err := w.handleOnAvgs(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 2, 3:
			// ===============[On]====================
			// 检查慢查询
			err := w.handleOnGlobal(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 4:
			// ===============[On]====================
			// 全表扫描大于2亿
			err := w.handleOnFscan(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 5:
			// ===============[On]====================
			// 队列超高消耗捕捉(TB级别)
			err := w.handleOnQueriesTB(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 6:
			// ===============[On]====================
			// 队列超高消耗捕捉(百亿扫描级别)
			err := w.handleOnQueriesMi(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 7:
			// ===============[On]====================
			//INSERT CATALOG 扫描数据量过大
			err := w.handleOnCatalog(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 8:
			// ===============[On]====================
			// 队列超高内存消耗捕捉
			err := w.handleOnQueriesGB(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		case 9:
			// ===============[On]====================
			// S3协议拦截
			err := w.handleOnkillS3(c.Connect, c.App, c.Fe, *c.Queries, c.Item, schema)
			if err != nil {
				util.Loggrs.Error(uid, c.Item.Id, " > ", err.Error())
			}
		default:

		}
	}
}
