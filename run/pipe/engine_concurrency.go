/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontend
 *@date    2024/8/8 15:29
 */

package pipe

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/robot"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

type logEntry struct {
	User string `json:"User"`
}

// OnConcurrencylimit
// 处理并发事件
func OnConcurrencylimit(app string) {
	if util.ConnectNorm.SlowQueryConcurrencylimit <= 0 {
		return
	}

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   "",
		Mode: "Concurrencylimit",
		Id:   "",
	})

	cos, err := engine.getmapConnect(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	var feips []map[string]interface{}
	r := cos.Raw("show frontends").Scan(&feips)
	if r.Error != nil {
		util.Loggrs.Error(uid, r.Error.Error())
		return
	}
	//var leader string
	//for _, m := range feips {
	//	if m["Role"].(string) == "LEADER" {
	//		leader = m["IP"].(string)
	//	}
	//}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := cos.DB()
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(10)                  //最大连接数
		sqlDB.SetMaxIdleConns(10)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	var result []string
	for _, feip := range feips {
		db, err := conn.StarRocksApp(app, feip["IP"].(string))
		if err != nil {
			util.Loggrs.Error(uid, r.Error.Error())
			continue
		}

		var m []map[string]interface{}
		r := db.Raw("show processlist").Scan(&m)
		if r.Error != nil {
			util.Loggrs.Error(uid, r.Error.Error())
			continue
		}
		for _, item := range m {
			marshal, _ := json.Marshal(item)
			if item["Command"].(string) == "Sleep" {
				continue
			}
			result = append(result, string(marshal))
		}

		/*每次使用完，主动关闭连接数*/
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(10)                  //最大连接数
		sqlDB.SetMaxIdleConns(10)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(10 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}

	for _, item := range entry(result) {
		username := strings.Split(item, ":")[0]
		conlimit, _ := strconv.Atoi(strings.Split(item, ":")[1])

		if conlimit < util.ConnectNorm.SlowQueryConcurrencylimit {
			continue
		}
		// get cache
		if _, ok := Concurrencylimit.Get(username); ok {
			continue
		}
		msg := fmt.Sprintf("并发预警：user:(%s) 发起的语句超过(%d)个,当前拦截阈值(%d)", username, conlimit, util.ConnectNorm.SlowQueryConcurrencylimit)
		util.Loggrs.Info(uid, msg)
		if util.Config.GetString("mode.Info") != "" {
			go robot.Send2Markdown("", msg, "", []string{util.Config.GetString("mode.Info")})
		}
		// set cache
		Concurrencylimit.Set(username, conlimit, cache.DefaultExpiration)
	}

	//
	//var mutex sync.Mutex
	////all
	//ch := make(chan map[string]int, 0)
	//go func() {
	//	m := make(map[string]int)
	//	for _, v := range s.QueriesAll {
	//		if m[v] == 0 {
	//			m[v] = 1
	//		} else {
	//			m[v]++
	//		}
	//	}
	//	ch <- m
	//}()
	//m := <-ch
	////running
	//chrun := make(chan map[string]int, 0)
	//go func() {
	//	run := make(map[string]int)
	//	for _, v := range s.QueriesRun {
	//		if run[v] == 0 {
	//			run[v] = 1
	//		} else {
	//			run[v]++
	//		}
	//	}
	//	chrun <- run
	//}()
	//run := <-chrun
	////penning
	//chpen := make(chan map[string]int, 0)
	//go func() {
	//	pen := make(map[string]int)
	//	for _, v := range s.QueriesPen {
	//		if pen[v] == 0 {
	//			pen[v] = 1
	//		} else {
	//			pen[v]++
	//		}
	//	}
	//	chpen <- pen
	//}()
	//pen := <-chpen
	//marshal, _ := json.Marshal(m)
	//util.Loggrs.Info(uid, fmt.Sprintf("%v", string(marshal)))
	//for k, v := range m {
	//	if k == "" {
	//		continue
	//	}
	//	util.Loggrs.Info(uid, fmt.Sprintf("检查并发 k:%s v:%d", k, v))
	//	if k == "svccnrpths" && v < 200 {
	//		continue
	//	}
	//	//白名单绕过
	//	if protect(k, &mutex) {
	//		util.Loggrs.Info(uid, k, " 白名单用户不做并发检查")
	//		return
	//	}
	//
	//	if v >= util.ConnectNorm.SlowQueryConcurrencylimit {
	//		util.Loggrs.Info(uid, "并发判断。")
	//		value, ok := s.Scache.Get(k)
	//		util.Loggrs.Info(uid, ok, value)
	//		if ok {
	//			continue
	//		}
	//		signs[s.App+k]++
	//		stime := time.Time{}
	//		if signs[s.App+k] < 2 {
	//			go func() {
	//				t := time.NewTicker(time.Second * 10)
	//				for {
	//					select {
	//					case <-t.C:
	//						if isTimeMoreThan5MinutesAgo(stime) {
	//							if signs[s.App+k] >= 1 {
	//								signs[s.App+k] = 0
	//							}
	//							return
	//						}
	//					}
	//				}
	//			}()
	//			continue
	//		}
	//		filename := fmt.Sprintf("%s/sql/%d", util.LogPath, time.Now().UnixMicro())
	//		threads(filename, s.App, k)
	//		/*发送告警*/
	//		url := fmt.Sprintf("http://%s:7890/log%s", util.H.Ip, filename)
	//
	//		var session, global string
	//		for _, app := range tools.UniqueMaps(util.ConnectRobot) {
	//			if app["type"].(string) == "global" {
	//				if app["robot"] != "" {
	//					global = app["robot"].(string)
	//				}
	//			}
	//			if app["key"].(string) == s.App {
	//				if app["robot"] != "" {
	//					session = app["robot"].(string)
	//				}
	//			}
	//		}
	//		ts := time.Now().Format("2006-01-02 15:04:05")
	//		var sign string
	//		if v >= util.ConnectNorm.SlowQueryConcurrencylimit {
	//			sign = "🔵"
	//		}
	//		if v >= util.ConnectNorm.SlowQueryConcurrencylimit*2 {
	//			sign = "\U0001F7E1"
	//		}
	//		if v >= util.ConnectNorm.SlowQueryConcurrencylimit*3 {
	//			sign = "🔴"
	//		}
	//		msgs := fmt.Sprintf(`[告警标题]：StarRocks并发告警\n[告警级别]：[%s]\n[告警时间]：[%s]\n[集群实例]：[%s]\n[集群账号]：[%s]\n[告警内容]：\n您好！系统监测到集群用户【%s】目前发起的查询已经达到了 [%d] 个，(可点击下面的log按钮进行查看) 具体如下：\n🟡- 当前并发\t：\t[%d]\n🟡- 设定阈值\t：\t[%d]\n🟡- RUNNING\t：\t[%d]\n🟡- PENDING\t：\t[%d]\n🟡- 持续时间\t：\t[2min]`,
	//			sign, ts, s.App, k, k,
	//			util.ConnectNorm.SlowQueryConcurrencylimit,
	//			v,
	//			util.ConnectNorm.SlowQueryConcurrencylimit, run[k], pen[k])
	//		util.Loggrs.Info(uid, msgs)
	//		robot.SendFsText("StarRocks并发告警", msgs, url, append(strings.Split(global, ","), session))
	//		s.Scache.Set(k, v, cache.DefaultExpiration)
	//		signs[s.App+k] = 0
	//	}
	//}
}

func entry(logData []string) []string {
	userCounts := make(map[string]int)

	for _, logEntryStr := range logData {
		var logEntry logEntry
		err := json.Unmarshal([]byte(logEntryStr), &logEntry)
		if err != nil {
			continue
		}
		userCounts[logEntry.User]++
	}
	var output []string
	for user, count := range userCounts {
		output = append(output, fmt.Sprintf("%s:%d", user, count))
	}
	return output
}

func findtry(logData []string, username string) []string {
	var data []string
	for _, logEntryStr := range logData {
		var logEntry logEntry
		err := json.Unmarshal([]byte(logEntryStr), &logEntry)
		if err != nil {
			continue
		}
		if logEntry.User == username {
			data = append(data, logEntryStr)
		}
	}
	return data
}
