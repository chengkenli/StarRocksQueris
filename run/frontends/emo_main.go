/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_main
 *@date    2024/11/6 14:05
 */

package fronends

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/util"
	"fmt"
	"sync"
)

func EmoIndex() {
	var (
		itemData []*util.SchemaData
		larkData []*util.Larkbodys
	)
	w := Workers{
		allTasks:     make(chan string),
		runningTasks: make(chan string),
		pendingTasks: make(chan string),
		lark:         make(chan *util.Larkbodys),
		data:         make(chan *util.SchemaData),
	}
	go func() {
		for {
			select {
			case lark := <-w.lark:
				larkData = append(larkData, lark)
			case data := <-w.data:
				itemData = append(itemData, data)
			}
		}
	}()

	var wg sync.WaitGroup
	for i, m := range util.ConnectBody {
		app := m["app"].(string)
		wg.Add(1)
		go func(i int, app string) {
			defer wg.Done()
			w.emomcluster(app)
		}(i, app)
	}
	wg.Wait()

	util.Loggrs.Info(fmt.Sprintf("Job -> feishu:[%d],item:[%d]", len(larkData), len(itemData)))
	if len(larkData) >= 1 {
		robot.SendFsCartBody(larkData)
	}
	if len(itemData) >= 1 && util.ConnectLink != nil {
		SessionAnalysisToSchema(util.ConnectLink, &itemData)
	}
	util.Loggrs.Info("Job done.")
}
