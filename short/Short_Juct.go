/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_juct
 *@date    2024/11/22 9:41
 */

package short

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
	"time"
)

func (j *Job) Juct(c *util.ReData, cannel chan struct{}) {
	for {
		select {
		case <-cannel:
			return
		case state := <-j.Donec:
			var ctime string
			if strings.Contains(c.Ctime, ",") {
				ctime = j.centerTransTime(c, 1)
			} else {
				ctime = c.Ctime
			}

			SinKey := strings.Split(state, "#")[0]
			SinMsg := strings.Split(state, "#")[1]

			switch SinKey {
			case c.App + "^open":
				loggrs.Info(uid, fmt.Sprintf("%s Receive >heartbeat,(r)[%s] %s ", SinMsg, ctime, c.Username))
			case c.App + "^close":
				loggrs.Info(uid, fmt.Sprintf("%s Receive >%s -> %s -> Offline", SinMsg, c.App, c.Username))
				//清空所有缓存
				loggrs.Info(uid, "所有缓存清空")
				healthOpen.DeleteExpired()

				k := c.App + c.Username
				for _, key := range applist {
					loggrs.Info(uid, "组合: ", k)
					loggrs.Info(uid, "数组: ", key)
					if k == key {
						if _, ok := healthOpen.Get(key); ok {
							loggrs.Info(uid, "剔除元素: ", key)
							healthOpen.Delete(key)
						}
					}
				}
				loggrs.Info(uid, "剔除数组: ", k)
				loggrs.Info(uid, "剔除前: ", applist)
				tools.RmInSlice(applist, c.App+c.Username)
				loggrs.Info(uid, "剔除后: ", applist)
				loggrs.Info(uid, "保障间距: ", ctime)
				p, v := shortBar(0, c.Ctime, ctime)

				if !strings.Contains(c.Ctime, ",") {
					if _, ok := onClose.Get(c.App + c.Username + time.Now().Format("2006-01-02")); ok {
						loggrs.Warn(uid, "关闭提示24小时内只发一次，在这之前已经做过发送，退出！")
						continue
					}
				}

				var logfile string
				if vl, ok := lastcache.Get(c.App + c.Username); ok {
					logfile = vl.(string)
				}

				j.Lark <- Body(
					&Portc{
						State:    0,
						App:      c.App,
						User:     c.Username,
						Timetr:   ctime,
						Comment:  fmt.Sprintf("维护结束，短查询保障机制(%s集群)保障时间已到或被关闭，已经释放强保障措施！", c.App),
						Resource: srcData,
						Core:     tools.RemoveDuplicateStrings(srcCore),
						ProceBar: p,
						ProceVal: v,
						Logfile:  logfile,
						SrcData:  c,
					})
				onClose.Set(c.App+c.Username+time.Now().Format("2006-01-02"), "on", cache.DefaultExpiration)

				// 重新传值，覆盖原有时间区间
				if strings.Contains(c.Ctime, ",") {
					loggrs.Info(uid, "发起重置时间区间请求")
					j.centerTransTime(c, 0)
				}
			}
		}
	}
}
