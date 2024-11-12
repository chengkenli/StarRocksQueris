/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_cluster
 *@date    2024/11/6 14:09
 */

package fronends

import (
	"StarRocksQueris/util"
	"fmt"
)

// 处理每个集群的总逻辑
func (w *Workers) emomcluster(app string) {
	/*初始化channel*/
	var alltask, runtask, pendtask []string
	/*用channel接收信号*/
	go func() {
		for {
			select {
			case all := <-w.allTasks:
				alltask = append(alltask, all)
			case running := <-w.runningTasks:
				runtask = append(runtask, running)
			case pending := <-w.pendingTasks:
				pendtask = append(pendtask, pending)
			}
		}
	}()

	for _, fe := range FronendNodes(app) {
		w.emofe(app, fe)
	}
	/*并发检查*/
	util.Loggrs.Info(fmt.Sprintf("%s Task -> allTasks:[%d],runningTasks:[%d],pendingTasks:[%d]", app, len(alltask), len(runtask), len(pendtask)))
	go func() {
		handleOnConcurrencylimit(
			&SlowHign{
				App:        app,
				Scache:     sessionConcurrencylimitCache,
				QueriesAll: alltask,
				QueriesRun: runtask,
				QueriesPen: pendtask,
			})
	}()
}
