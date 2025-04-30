/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontend
 *@date    2024/8/8 15:29
 */

package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
	"sync"
	"time"
)

type SlowHign struct {
	App        string
	Scache     *cache.Cache
	QueriesAll []string
	QueriesRun []string
	QueriesPen []string
}

var signs = make(map[string]int)

// 处理并发事件
func handleOnConcurrencylimit(s *SlowHign) {
	uid := xid.Xid(&xid.Uid{
		App:  s.App,
		Fe:   "",
		Mode: "Concurrencylimit",
		Id:   "",
	})
	var mutex sync.Mutex
	if s.App == "cdp" || s.App == "api" || s.App == "ma" {
		return
	}
	if util.ConnectNorm.SlowQueryConcurrencylimit <= 0 {
		return
	}
	//all
	ch := make(chan map[string]int, 0)
	go func() {
		m := make(map[string]int)
		for _, v := range s.QueriesAll {
			if m[v] == 0 {
				m[v] = 1
			} else {
				m[v]++
			}
		}
		ch <- m
	}()
	m := <-ch
	//running
	chrun := make(chan map[string]int, 0)
	go func() {
		run := make(map[string]int)
		for _, v := range s.QueriesRun {
			if run[v] == 0 {
				run[v] = 1
			} else {
				run[v]++
			}
		}
		chrun <- run
	}()
	run := <-chrun
	//penning
	chpen := make(chan map[string]int, 0)
	go func() {
		pen := make(map[string]int)
		for _, v := range s.QueriesPen {
			if pen[v] == 0 {
				pen[v] = 1
			} else {
				pen[v]++
			}
		}
		chpen <- pen
	}()
	pen := <-chpen
	marshal, _ := json.Marshal(m)
	util.Loggrs.Info(uid, fmt.Sprintf("%v", string(marshal)))
	for k, v := range m {
		if k == "" {
			continue
		}
		util.Loggrs.Info(uid, fmt.Sprintf("检查并发 k:%s v:%d", k, v))
		if k == "svccnrpths" && v < 200 {
			continue
		}
		//白名单绕过
		if protect(k, &mutex) {
			return
		}

		if v >= util.ConnectNorm.SlowQueryConcurrencylimit {
			util.Loggrs.Info(uid, "并发判断。")
			value, ok := s.Scache.Get(k)
			util.Loggrs.Info(uid, ok, value)
			if ok {
				continue
			}
			signs[s.App+k]++
			//msg = fmt.Sprintf(`🔂连接并发过多告警\n💬告警类型\t: [连接并发数过高]\n💬集群名称\t: [%s]\n💬告警对象\t: [%s]\n💬触发阈值\t: [>%d]\n💬现并发数\t: [%d]\n💬当前队列\t: [%d]\n💬RUNNING\t: [%d]\n💬PENNING\t: [%d]\n💬持续时间\t: [2min]\n`,
			//	s.App,
			//	k,
			//	util.ConnectNorm.SlowQueryConcurrencylimit,
			//	v,
			//	len(s.QueriesAll),
			//	run[k],
			//	pen[k],
			//)
			//
			//// end
			//util.Loggrs.Info(fmt.Sprintf("%s, [%s]出现了并发，出现次数=[%d], %s", s.App, k, signs[s.App+k], msg))

			stime := time.Time{}
			if signs[s.App+k] < 2 {
				go func() {
					t := time.NewTicker(time.Second * 10)
					for {
						select {
						case <-t.C:
							if isTimeMoreThan5MinutesAgo(stime) {
								if signs[s.App+k] >= 1 {
									signs[s.App+k] = 0
								}
								return
							}
						}
					}
				}()
				continue
			}
			filename := fmt.Sprintf("%s/sql/%d", util.LogPath, time.Now().UnixMicro())
			threads(filename, s.App, k)
			/*发送告警*/
			url := fmt.Sprintf("http://%s:7890/log%s", util.H.Ip, filename)

			var session, global string
			for _, app := range tools.UniqueMaps(util.ConnectRobot) {
				if app["type"].(string) == "global" {
					if app["robot"] != "" {
						global = app["robot"].(string)
					}
				}
				if app["key"].(string) == s.App {
					if app["robot"] != "" {
						session = app["robot"].(string)
					}
				}
			}
			ts := time.Now().Format("2006-01-02 15:04:05")
			var sign string
			if v >= util.ConnectNorm.SlowQueryConcurrencylimit {
				sign = "🔵"
			}
			if v >= util.ConnectNorm.SlowQueryConcurrencylimit*2 {
				sign = "\U0001F7E1"
			}
			if v >= util.ConnectNorm.SlowQueryConcurrencylimit*3 {
				sign = "🔴"
			}
			msgs := fmt.Sprintf(`[告警标题]：StarRocks并发告警\n[告警级别]：[%s]\n[告警时间]：[%s]\n[集群实例]：[%s]\n[集群账号]：[%s]\n[告警内容]：\n您好！系统监测到集群用户【%s】目前发起的查询已经达到了 [%d] 个，(可点击下面的log按钮进行查看) 具体如下：\n🟡- 当前并发\t：\t[%d]\n🟡- 设定阈值\t：\t[%d]\n🟡- RUNNING\t：\t[%d]\n🟡- PENDING\t：\t[%d]\n🟡- 持续时间\t：\t[2min]`,
				sign, ts, s.App, k, k,
				util.ConnectNorm.SlowQueryConcurrencylimit,
				v,
				util.ConnectNorm.SlowQueryConcurrencylimit, run[k], pen[k])
			util.Loggrs.Info(uid, msgs)
			robot.SendFsText("StarRocks并发告警", msgs, url, append(strings.Split(global, ","), session))
			s.Scache.Set(k, v, cache.DefaultExpiration)
			signs[s.App+k] = 0
		}
	}
}

// 判断时间是否过了5分钟
func isTimeMoreThan5MinutesAgo(t time.Time) bool {
	// 获取当前时间
	now := time.Now()
	// 计算给定时间t与当前时间的时间差
	diff := now.Sub(t)
	// 检查时间差是否大于5分钟
	return diff > 5*time.Minute
}

func threads(filename, app, user string) {
	for _, fe := range FronendNodes(app) {
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
			sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
			sqlDB.Close()
		}()

		var p util.Process
		r := db.Raw("show full processlist").Scan(&p)
		if r.Error != nil {
			util.Loggrs.Error(r.Error.Error())
			return
		}
		for _, s := range p {
			if s.Command == "Query" && s.User == user {
				marshal, _ := json.Marshal(&s)
				tools.WriteFile(filename, fmt.Sprintf("%v", string(marshal)))
				tools.WriteFile(filename, "\n##############################################################################\n\n\n")
			}
		}
	}
}
