/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendSessionProcessList
 *@date    2024/8/8 13:15
 */

package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/robfig/cron/v3"
	"strings"
	"sync"
	"time"
)

type QueriesInfo struct {
	App                   string
	Fe                    string
	Running, Penning, All []string
}

var (
	sessionConcurrencylimitCache = cache.New(5*time.Minute, 10*time.Minute)
	FsCache                      = cache.New(time.Duration(util.QueryTime)*time.Second, time.Duration(util.QueryTime)*2*time.Second)
	EmCache                      = cache.New(time.Duration(util.QueryTime)*time.Second, time.Duration(util.QueryTime)*2*time.Second)
	DoneC                        = make(chan []*util.Larkbodys)
)

func SessionDebug() {
	doneC := make(chan struct{})
	go func() {
		for {
			select {
			case <-doneC:
				return
			}
		}
	}()
	// 飞书告警推送
	go func() {
		for {
			select {
			case <-DoneC:
				robot.SendFsCartBody(<-DoneC)
			}
		}
	}()

	fmt.Println("开始")
	// job stsrt
	var itemData []*util.SchemaData
	var fsbody []*util.Larkbodys
	var wg sync.WaitGroup

	for i, m := range util.ConnectBody {
		app := m["app"].(string)
		wg.Add(1)
		go func(i int, app string) {
			defer wg.Done()
			// 启动主逻辑
			idx, body := SessionProcessList(app)
			if idx == nil {
				return
			}
			itemData = append(itemData, idx...)
			fsbody = append(fsbody, body...)
		}(i, app)
	}
	wg.Wait()
	// 飞书告警
	if fsbody != nil {
		robot.SendFsCartBody(fsbody)
	}
	// 慢查询落表

	if itemData != nil && util.ConnectLink != nil {
		SessionAnalysisToSchema(util.ConnectLink, &itemData)
	}
	fmt.Println("结束")
	doneC <- struct{}{}
	// job end
}

func SessionApp() {
	crontab := cron.New()
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc("*/2 * * * *", func() {
		// job stsrt
		var itemData []*util.SchemaData
		var fsbody []*util.Larkbodys
		var wg sync.WaitGroup

		for i, m := range util.ConnectBody {
			app := m["app"].(string)
			wg.Add(1)
			go func(i int, app string) {
				defer wg.Done()
				// 启动主逻辑
				idx, body := SessionProcessList(app)
				if idx == nil {
					return
				}
				itemData = append(itemData, idx...)
				fsbody = append(fsbody, body...)
			}(i, app)
		}
		wg.Wait()
		// 飞书告警
		if fsbody != nil {
			robot.SendFsCartBody(fsbody)
		}
		// 慢查询落表

		if itemData != nil && util.ConnectLink != nil {
			SessionAnalysisToSchema(util.ConnectLink, &itemData)
		}
		// job end
	})
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	// 启动定时器
	crontab.Start()
	// 定时任务是另起协程执行的,这里使用 select 简答阻塞.实际开发中需要
	// 根据实际情况进行控制
	select {}
}

// SessionProcessList 处理每个集群的总逻辑
func SessionProcessList(app string) ([]*util.SchemaData, []*util.Larkbodys) {
	var (
		runs, pens, alls []string
		idx              []*util.SchemaData
		fsbody           []*util.Larkbodys
	)
	for _, fe := range FronendNodes(app) {
		// 第一核心逻辑
		run, pen, all, itemdata, body := handleOnFe(
			&QueriesInfo{
				App: app,
				Fe:  fe,
			})
		runs = append(runs, run...)
		pens = append(pens, pen...)
		alls = append(alls, all...)

		if itemdata != nil {
			idx = append(idx, itemdata...)
		}
		if body != nil {
			fsbody = append(fsbody, body...)
		}
	}

	if !util.P.Check {
		util.Loggrs.Info(fmt.Sprintf("!!!!!!!!!! %d [done].查询队列状态 %s Queries:[%d]，Running:[%d]，Penning:[%d]", len(fsbody), app, len(alls), len(runs), len(pens)))
		//并发检测
		handleOnConcurrencylimit(
			&SlowHign{
				App:        app,
				Scache:     sessionConcurrencylimitCache,
				QueriesAll: alls,
				QueriesRun: runs,
				QueriesPen: pens,
			})
	}

	return idx, fsbody
}

// handleOnFe 筛选每个fe中的查询
func handleOnFe(s *QueriesInfo) ([]string, []string, []string, []*util.SchemaData, []*util.Larkbodys) {
	var all []string
	defer func() {
		if r := recover(); r != any(nil) {
			util.Loggrs.Panic(fmt.Sprintf("%v", r))
		}
	}()

	db, err := conn.StarRocksApp(s.App, s.Fe)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil, nil, nil, nil, nil
	}
	defer func() {
		// 设置数据库连接池
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		sqlDB.SetConnMaxLifetime(time.Second * 60)
		sqlDB.SetMaxOpenConns(100)
		sqlDB.SetMaxIdleConns(50)
	}()

	// 获取当前fe队列信息
	queries := UriCurrentQueries(s.App, s.Fe)
	if !util.P.Check {
		util.Loggrs.Info(fmt.Sprintf("4 %s --------->>>debug:%v", s.Fe, queries))
	}

	// 获取当前fe所有session sql
	var p util.Process
	r := db.Raw("show full processlist").Scan(&p)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return nil, nil, nil, nil, nil
	}
	if p == nil {
		return nil, nil, nil, nil, nil
	}

	var Idx []*util.SchemaData
	var fsbody []*util.Larkbodys

	ch := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, item := range p {
		wg.Add(1)
		item := item
		go func() {
			defer func() {
				if len(fsbody) != 0 {
					DoneC <- fsbody
				}
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
					s.Penning = append(s.Penning, item.User)
				}
				if item.IsPending == "false" {
					s.Running = append(s.Running, item.User)
				}
			}
			if item.User != "" {
				all = append(all, item.User)
			}
			if item.State == "ERR" {
				util.Loggrs.Info(fmt.Sprintf("进程中语句已经异常中断，进行清退：%s %s %s", s.App, item.Id, item.User))
				Onkill(0, s.App, s.Fe, item.Id)
				return
			}

			// 判断当前用户是否为百名单，白名单不进行处理
			if util.ConnectNorm["slow_query_focususer"] != nil {
				if tools.StringInSlice(item.User, strings.Split(util.ConnectNorm["slow_query_focususer"].(string), ",")) {
					return
				}
			}

			////检查异常参数
			//body2, itemdata2, err := handleOnAvgs(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if itemdata2 != nil {
			//	Idx = append(Idx, itemdata2)
			//}
			//if body2 != nil {
			//	fsbody = append(fsbody, body2)
			//}
			//
			//// 全表扫描大于2亿
			//body3, itemdata3, err := handleOnFscan(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if itemdata3 != nil {
			//	Idx = append(Idx, itemdata3)
			//}
			//if body3 != nil {
			//	fsbody = append(fsbody, body3)
			//}
			//
			//// 检查慢查询
			//body, itemdata, err := handleOnGlobal(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if itemdata != nil {
			//	Idx = append(Idx, itemdata)
			//}
			//if body != nil {
			//	fsbody = append(fsbody, body)
			//}
			//
			//// 队列超高消耗捕捉(TB级别)
			//body4, itemdata4, err := handleOnQueriesTB(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if itemdata4 != nil {
			//	Idx = append(Idx, itemdata4)
			//}
			//if body4 != nil {
			//	fsbody = append(fsbody, body4)
			//}
			//
			//// 队列超高消耗捕捉(百万扫描级别)
			//body5, sdata5, err := handleOnQueriesMi(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if sdata5 != nil {
			//	Idx = append(Idx, sdata5)
			//}
			//if body5 != nil {
			//	fsbody = append(fsbody, body5)
			//}
			//
			////INSERT CATALOG 扫描数据量过大
			//body6, sdata6, err := handleOnCatalog(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if sdata6 != nil {
			//	Idx = append(Idx, sdata6)
			//}
			//if body6 != nil {
			//	fsbody = append(fsbody, body6)
			//}
			//
			//// 队列超高内存消耗捕捉
			//body7, sdata7, err := handleOnQueriesGB(db, s.App, s.Fe, queries, (*util.Process2)(&item))
			//if err != nil {
			//	util.Loggrs.Error(err.Error())
			//	return
			//}
			//if sdata7 != nil {
			//	Idx = append(Idx, sdata7)
			//}
			//if body7 != nil {
			//	fsbody = append(fsbody, body7)
			//}

		}()
	}
	wg.Wait()

	return s.Running, s.Penning, all, Idx, fsbody
}
