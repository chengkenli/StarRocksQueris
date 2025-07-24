/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_mfe
 *@date    2024/11/6 14:12
 */

package pipe

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"sync"
	"time"
)

// 处理每个单独的fe中的事务
func (w *Workers) emofe(app, fe string) {

	db, err := conn.StarRocksApp(app, fe)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(60 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()

	// 获取当前fe队列信息
	queries := UriCurrentQueries(app, fe)

	// 获取当前fe所有session sql
	var p util.Process
	r := db.Raw("show full processlist").Scan(&p)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	if len(p) == 0 {
		return
	}

	ch := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, item := range p {
		wg.Add(1)
		item := item
		go func() {
			defer func() {
				<-ch
				wg.Done()
			}()

			ch <- struct{}{}
			// 队列数量整合
			if item.Command != "Query" {
				return
			}

			if item.IsPending != "" {
				if item.IsPending == "true" {
					w.pendingTasks <- item.User
				} else {
					w.runningTasks <- item.User
				}
			}
			if item.User != "" {
				w.allTasks <- item.User
			}

			// 白名单逻辑
			var mutex sync.Mutex
			if protect(item.User, &mutex) {
				go w.handleOnCore(db, app, fe, queries, (*util.Process2)(&item), nil)
				return
			}

			w.emomhandle(&handle{
				Connect: db,
				App:     app,
				Fe:      fe,
				Queries: &queries,
				Item:    (*util.Process2)(&item),
			}, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

		}()
	}
	wg.Wait()
}
