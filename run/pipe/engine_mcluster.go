/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_cluster
 *@date    2024/11/6 14:09
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
)

// 处理每个集群的总逻辑
func (w *Workers) emomcluster(app string) {
	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   "",
		Mode: "",
		Id:   "",
	})
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

	for _, fe := range fronendNodes(app) {
		w.emofe(app, fe)
	}
	util.Loggrs.Info(uid, "fe事务处理完毕")
	/*并发检查*/
	util.Loggrs.Info(uid, fmt.Sprintf("allTasks:[%d],runningTasks:[%d],pendingTasks:[%d]", len(alltask), len(runtask), len(pendtask)))
	//go func() {
	//	OnConcurrencylimit(
	//		&SlowHign{
	//			App:        app,
	//			Scache:     climitcache,
	//			QueriesAll: alltask,
	//			QueriesRun: runtask,
	//			QueriesPen: pendtask,
	//		})
	//}()
}
