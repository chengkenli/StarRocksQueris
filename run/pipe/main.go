/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_main
 *@date    2024/11/6 14:05
 */

package pipe

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/run/clientip"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"sync"
)

func index() {
	var (
		itemData   []*util.SchemaData
		larkData   []*util.Larkbodys
		clientData []*util.ClientIPData
	)
	// 初始化channel
	w := Workers{
		allTasks:     make(chan string),
		runningTasks: make(chan string),
		pendingTasks: make(chan string),
		lark:         make(chan *util.Larkbodys),
		data:         make(chan *util.SchemaData),
		clientDatas:  make(chan *util.ClientIPData),
	}
	uid := xid.Xid(nil)
	// channel实时等待结果返回
	go func() {
		for {
			select {
			case lark := <-w.lark:
				if !existsLarkbodys(larkData, lark) {
					larkData = append(larkData, lark)
					util.Loggrs.Info(uid, "Job -> lark length:", len(larkData))
				}
			case data := <-w.data:
				if !existsSchemaData(itemData, data) {
					itemData = append(itemData, data)
					util.Loggrs.Info(uid, "Job -> item length:", len(itemData))
				}
			case client := <-w.clientDatas:
				if !existsClientData(clientData, client) {
					clientData = append(clientData, client)
					util.Loggrs.Info(uid, "Job -> client length:", len(clientData))
				}
			}
		}
	}()

	// goroutine 主体结构
	var wg sync.WaitGroup
	apps := tools.UniqueMaps(util.ConnectBody)
	util.Loggrs.Info(uid, "Job -> ", len(apps))
	for i, m := range tools.UniqueMaps(util.ConnectBody) {
		app := m["app"].(string)
		wg.Add(1)
		go func(i int, app string) {
			defer wg.Done()
			// 设计一个定时器，每10秒执行扫描查询队列，为更好的查杀拦截

			w.emomcluster(app)
		}(i, app)
	}
	wg.Wait()

	util.Loggrs.Info(uid, fmt.Sprintf("Job -> feishu:[%d],item:[%d]", len(larkData), len(itemData)))
	if len(larkData) >= 1 {
		robot.SendFsCartApp2Group(larkData)
	}
	if len(itemData) >= 1 && util.ConnectLink != nil {
		SessionAnalysisToSchema(util.ConnectLink, &itemData)
	}
	if len(clientData) >= 1 && util.ConnectLink != nil {
		clientip.ClientStreamload(util.ConnectLink, &clientData)
	}
	util.Loggrs.Info(uid, "Job done.")
}
