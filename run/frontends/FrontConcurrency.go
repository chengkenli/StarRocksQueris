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
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
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
	if util.ConnectNorm["slow_query_concurrencylimit"] == nil {
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

	util.Loggrs.Info(fmt.Sprintf("%v", m))
	for k, v := range m {
		if k == "" {
			continue
		}
		util.Loggrs.Info(s.App, fmt.Sprintf("检查并发 k:%s v:%d", k, v))
		if k == "svccnrpths" && v < 200 {
			continue
		}

		if v >= int(util.ConnectNorm["slow_query_concurrencylimit"].(int32)) {
			_, ok := s.Scache.Get(k)
			if !ok {
				signs[s.App+k]++
				var msg string
				msg = fmt.Sprintf(`🔂连接并发过多告警\n💬告警类型\t: [连接并发数过高]\n💬集群名称\t: [%s]\n💬告警对象\t: [%s]\n💬触发阈值\t: [>%d]\n💬现并发数\t: [%d]\n💬当前队列\t: [%d]\n💬RUNNING\t: [%d]\n💬PENNING\t: [%d]\n💬持续时间\t: [2min]\n`,
					s.App,
					k,
					util.ConnectNorm["slow_query_concurrencylimit"].(int32),
					v,
					len(s.QueriesAll),
					run[k],
					pen[k],
				)

				// end
				util.Loggrs.Info(fmt.Sprintf("%s, [%s]出现了并发，出现次数=[%d], %s", s.App, k, signs[s.App+k], msg))

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
				filename := fmt.Sprintf("%s/%d", util.LogPath, time.Now().UnixMicro())
				threads(filename, s.App, k)
				util.Loggrs.Info("发送告警  ->   " + msg)
				/*发送告警*/
				url := fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, filename)

				var session, global string
				for _, app := range util.ConnectRobot {
					if app["type"].(string) == "global" {
						if v1, ok2 := app["robot"]; ok2 {
							global = v1.(string)
						}
					}
					if app["key"].(string) == s.App {
						if v1, ok2 := app["robot"]; ok2 {
							session = v1.(string)
						}
					}
				}
				robot.SendFsText("", msg, url, append(strings.Split(global, ","), session))
				s.Scache.Set(k, v, cache.DefaultExpiration)
				signs[s.App+k] = 0
			}
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
