/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_mhandle
 *@date    2024/11/6 14:52
 */

package fronends

import (
	"StarRocksQueris/util"
	"sync"
)

// 处理每个拦截模式
func (w *Workers) emomhandle(c *handle, p ...interface{}) {
	var mutex sync.Mutex
	for _, i2 := range p {
		switch i2.(int) {
		case 0:
			// 进程中语句已经异常中断，进行清退
			err := w.handleOnStatus(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 1:
			if protect(c.Item.User, &mutex) {
				return
			}
			//检查异常参数
			err := w.handleOnAvgs(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 2, 3:
			if protect(c.Item.User, &mutex) {
				return
			}
			// 检查慢查询
			err := w.handleOnGlobal(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 4:
			if protect(c.Item.User, &mutex) {
				return
			}
			// 全表扫描大于2亿
			err := w.handleOnFscan(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 5:
			if protect(c.Item.User, &mutex) {
				return
			}
			// 队列超高消耗捕捉(TB级别)
			err := w.handleOnQueriesTB(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 6:
			if protect(c.Item.User, &mutex) {
				return
			}
			// 队列超高消耗捕捉(百亿扫描级别)
			err := w.handleOnQueriesMi(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 7:
			if protect(c.Item.User, &mutex) {
				return
			}
			//INSERT CATALOG 扫描数据量过大
			err := w.handleOnCatalog(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		case 8:
			if protect(c.Item.User, &mutex) {
				return
			}
			// 队列超高内存消耗捕捉
			err := w.handleOnQueriesGB(c.Connect, c.App, c.Fe, *c.Queries, c.Item)
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
		default:

		}
	}
}
