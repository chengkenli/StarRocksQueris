/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_ticker
 *@date    2024/11/22 9:41
 */

package short

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
	"sync"
	"time"
)

func (j *Job) ticker(c *util.ReData, cannel chan struct{}) {
	logfile := fmt.Sprintf("%s/sql/%s_%s_%s_%d.html", util.LogPath, c.App, c.Username, "sum", time.Now().UnixNano())
	lastcache.Set(c.App+c.Username, logfile, cache.DefaultExpiration)

	// 个人
	db, err := conn.StarRocksShort(c)
	if err != nil {
		loggrs.Error(uid, err.Error())
		<-cannel
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
		loggrs.Warn(uid, "释放StarRocks连接！")
	}()
	// 全局
	global, err := conn.StarRocks(c.App)
	if err != nil {
		loggrs.Error(uid, err.Error())
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()

	if _, ok := isTime(j.centerTransTime(c, 1)); ok {
		Resource(c, global)
	}

	var i, y, x int
	var once, once2 sync.Once
	Ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-cannel:
			loggrs.Warn(uid, "disconnect~")
			return
		case <-Ticker.C:

			if _, ok := lastcache.Get(c.App + c.Username); !ok {
				logfile = fmt.Sprintf("%s/sql/%s_%s_%s_%d.html", util.LogPath, c.App, c.Username, "sum", time.Now().UnixNano())
				lastcache.Set(c.App+c.Username, logfile, cache.DefaultExpiration)
			}

			// 定时重塑
			var ctime string
			if strings.Contains(c.Ctime, ",") {
				ctime = j.centerTransTime(c, 1)
			} else {
				ctime = c.Ctime
			}

			if i%500 == 0 {
				loggrs.Info(uid, fmt.Sprintf("Check   >%s [%s]%s(%d)(%d)(%d)", c.Username, c.Ctime, ctime, i, y, x))
			}

			if i%10 == 0 {
				loggrs.Info(uid, fmt.Sprintf("crond   >定时播报时间: %s > %s", ctime, c.Username))
			}

			i++
			//判断时间段
			if !strings.Contains(ctime, "-") {
				loggrs.Warn(uid, "1 ctime is non-standard，follow the format：09:00-18:00")
				continue
			}
			split := strings.Split(ctime, "-")
			if len(split) < 2 {
				loggrs.Warn(uid, "2 ctime is non-standard，follow the format：09:00-18:00")
				continue
			}

			//周末不进行心跳维护
			//switch time.Now().Weekday() {
			//case time.Saturday:
			//	if i%500 == 0 {
			//		loggrs.Info(time.Saturday.String(), " 默认不维护！")
			//	}
			//	continue
			//case time.Sunday:
			//	if i%500 == 0 {
			//		loggrs.Info(time.Sunday.String(), " 默认不维护！")
			//	}
			//	continue
			//}
			//start
			_, ok := isTime(ctime)
			if !ok {
				// 判断当前时间是否等于结束时间，如果等于，那么发起close channel
				if time.Now().Format("15:04") == strings.Split(ctime, "-")[1] {
					once2.Do(func() {
						healthOpen.Delete(c.App + c.Username)
						tools.RemoveInSlice(applist, c.App+c.Username)
						j.Donec <- c.App + "^close#" + fmt.Sprintf("Close   >(%v)[%s] (%d)(%d)(%d) - %s ", "=", ctime, i, y, x, c.Username)
					})
				}
				continue
			}
			// 如果又到了判断时间，那么重置once2
			once2 = sync.Once{}

			y++
			//实施心跳
			var m map[string]interface{}
			r := db.Raw("select 1 as hb").Scan(&m)
			if r.Error != nil {
				loggrs.Error(uid, r.Error.Error())
				continue
			}
			v, ok := m["hb"]
			if ok {
				x++
				j.Donec <- c.App + "^open#" + fmt.Sprintf("Submit  >(%v)[%s] (%d)(%d)(%d) - %s ", v.(interface{}), ctime, i, y, x, c.Username)
				// 把指标加入到切片中
				if !tools.StringInSlice(c.App+c.Username, applist) {
					applist = append(applist, c.App+c.Username)
				}
				var state int
				var warn string
				if x > 10 {
					state = 2
					warn = fmt.Sprintf("叮~ 定时播报，短查询保障机制(%s集群)正常维护中。", c.App)
				}
				once.Do(func() {
					state = 1
					warn = fmt.Sprintf("初始化启动，短查询保障机制(%s集群)进入重点保障时间段。", c.App)
				})

				if strings.Contains(warn, "初始化启动") && c.Init == 0 {
					healthOpen.Set(c.App+c.Username, "", cache.DefaultExpiration)
					j.Donec <- c.App + "^open#" + fmt.Sprintf("Init    >(%v)[%s] (%d)(%d)(%d) - %s ", v.(interface{}), ctime, i, y, x, c.Username)
					continue
				}
				if len(warn) == 0 {
					healthOpen.Set(c.App+c.Username, "", cache.DefaultExpiration)
					j.Donec <- c.App + "^open#" + fmt.Sprintf("Send    >(%v)[%s] (%d)(%d)(%d) - %s ", v.(interface{}), ctime, i, y, x, c.Username)
					continue
				}
				// 获取缓存状态
				if _, ok := healthOpen.Get(c.App + c.Username); !ok {
					if _, ok := isTime(j.centerTransTime(c, 1)); ok {
						Resource(c, global)
					}
					p, v := shortBar(1, c.Ctime, ctime)
					j.Lark <- Body(
						&Portc{
							State:    state,
							App:      c.App,
							User:     c.Username,
							Timetr:   ctime,
							Comment:  warn,
							Resource: srcData,
							Core:     tools.RemoveDuplicateStrings(srcCore),
							ProceBar: p,
							ProceVal: v,
							Logfile:  logfile,
							SrcData:  c,
						})
					healthOpen.Set(c.App+c.Username, "", cache.DefaultExpiration)
					j.Donec <- c.App + "^open#" + fmt.Sprintf("Send    >(%v)[%s] (%d)(%d)(%d) - %s ", "", ctime, i, y, x, c.Username)
				}
			} else {
				loggrs.Info(uid, "账号已经关闭，清理缓存！")
				healthOpen.Delete(c.App + c.Username)
				j.Donec <- c.App + "^close#" + fmt.Sprintf("Close   >(%v)[%s] (%d)(%d)(%d) - %s ", v.(interface{}), ctime, i, y, x, c.Username)
			}
		}
	}
}
