/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    engine_OnGlobalQueris
 *@date    2025/7/18 9:49
 */

package pipe

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"context"
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	globalLark = make(chan *util.Larkbodys)
	globalData = make(chan *util.SchemaData)
)

func init() {
	go func() {
		for {
			select {
			case larkData := <-globalLark:
				go robot.SendFsCartApp2Group([]*util.Larkbodys{larkData})
			case loadData := <-globalData:
				go SessionAnalysisToSchema(util.ConnectLink, &[]*util.SchemaData{loadData})
			}
		}
	}()
}

func (engine *threadMap) OnGlobalQueries() {
	engine._init()
	util.Loggrs.Info("[ok] 初始化查询队列监控矩阵")
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ticker.C:
			// goroutine 主体结构
			for _, m := range tools.UniqueMaps(util.MetaLink) {
				app := m["app"].(string)
				db, err := engine.getmapConnect(app)
				if err != nil {
					util.Loggrs.Error(err.Error())
					continue
				}
				if tools.Version(db) < 3.3 {
					continue
				}
				if app != "sr-adhoc" {
					continue
				}

				go OnGlobalRun(app, db)
			}
		}
	}

}

func OnGlobalRun(app string, db *gorm.DB) {
	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   "",
		Mode: "OnGlobalRun",
		Id:   "",
	})
	var m []util.GlobalQueries
	r := db.Raw("show proc '/global_current_queries'").Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(uid, r.Error.Error())
		return
	}
	done := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, item := range sortByScanRowsDesc(m) {
		wg.Add(1)
		go func(item util.GlobalQueries) {
			defer func() {
				<-done
				wg.Done()
			}()

			done <- struct{}{}

			if item.ConnectionId <= 0 {
				return
			}
			execTime, err := parseCustomDuration(item.ExecTime)
			if err != nil {
				util.Loggrs.Error(err.Error())
				return
			}
			util.Loggrs.Info("查询队列监控矩阵:", execTime, ",", item)
			if execTime < 10 {
				return
			}

			ScanBytes, _ := strconv.ParseFloat(item.ScanBytes, 64)
			ScanRows := Int64(item.ScanRows)

			if ScanRows < util.ConnectNorm.SlowQueryFrontendScanrows && ScanBytes < float64(util.ConnectNorm.SlowQueryFrontendScanbytes) {
				return
			}
			// main
			var killSign int
			var warnmesg string
			if ScanRows >= util.ConnectNorm.SlowQueryFrontendScanrows {
				killSign = 6
				warnmesg = "(队列矩阵)扫描行数已经达到百亿级别，需整改限制分区缩小范围，避免继续触发拦截！"
			}
			if ScanBytes >= float64(util.ConnectNorm.SlowQueryFrontendScanbytes) {
				killSign = 5
				warnmesg = "(队列矩阵)扫描消耗字节已超TB+消耗极大内存，需整改限制分区缩小查询范围，避免继续触发拦截！"
			}
			util.Loggrs.Info(uid, "发现情况进行汇报:", warnmesg)
			marshal, _ := json.Marshal(item)
			util.Loggrs.Info(uid, string(marshal))

			OnScanModel(app, warnmesg, killSign, &item)

		}(item)
	}
	wg.Wait()
}

// 将 "2min:48s"、"10s"、"36.519 s" 转换为秒数（int）
func parseCustomDuration(durationStr string) (int, error) {
	// 预处理字符串：
	// 1. 替换 "min:" 为 "m"（Go 的 Duration 格式要求）
	// 2. 去掉空格和多余的冒号
	normalized := strings.ReplaceAll(durationStr, "min:", "m")
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, ":", "")

	// 解析为 time.Duration
	duration, err := time.ParseDuration(normalized)
	if err != nil {
		return 0, fmt.Errorf("解析时间失败: %v", err)
	}
	// 转换为秒（int），四舍五入
	return int(duration.Seconds() + 0.5), nil
}
func OnScanModel(app, warnmesg string, killsign int, item *util.GlobalQueries) {
	feip, prolist, err := getConnectionId(app, fmt.Sprintf("%d", item.ConnectionId))
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	nodes := UriCurrentQueriesHosts(app, feip, item.QueryId)
	stmts := UriCurrentQueriesStmt(app, feip, item.QueryId)
	schema, _ := SessionSchemaRegexp(stmts)

	if stmts == "" {
		return
	}

	// 去拿一个数据库连接给到它
	db, err := engine.getmapConnect(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}

	body, sdata := InQueris(
		&util.InQue{
			Opinion: warnmesg,
			Sign:    Singnel(killsign),
			Nature:  "intercept",
			App:     app,
			Schema:  schema,
			Fe:      feip,
			Queris: &util.SessionBigQuery{
				Db:            db,
				LogFile:       fmt.Sprintf("%s/sql/%s_%d_%s_%d.sql", util.LogPath, item.User, item.ConnectionId, item.ExecTime, time.Now().UnixNano()),
				StartTime:     item.StartTime,
				QueryId:       item.QueryId,
				ConnectionId:  fmt.Sprintf("%d", item.ConnectionId),
				Database:      item.Database,
				User:          item.User,
				ScanBytes:     item.ScanBytes,
				ScanRows:      item.ScanRows,
				MemoryUsage:   item.MemoryUsage,
				DiskSpillSize: item.DiskSpillSize,
				CPUTime:       item.CPUTime,
				ExecTime:      item.ExecTime,
				Nodes:         nodes,
				Stmt:          stmts,
			},
			Larkcache:  LarkConnectionId,
			Emailcache: MailConnectionId,
			Item:       &prolist,
			Action:     killsign,
			Logfile:    fmt.Sprintf("%s/sql/%s_%d_%s_%d.sql", util.LogPath, item.User, item.ConnectionId, item.ExecTime, time.Now().UnixNano()),
			Iceberg:    "",
			Explog:     "",
		})

	if item.ConnectionId <= 0 {
		return
	}

	go Onkill(killsign, app, feip, fmt.Sprintf("%d", item.ConnectionId))
	globalLark <- body
	globalData <- sdata
}

func sortByScanRowsDesc(queries []util.GlobalQueries) []util.GlobalQueries {
	// 复制切片以避免修改原数据
	sorted := make([]util.GlobalQueries, len(queries))
	copy(sorted, queries)

	// 按 ScanRows 降序排序
	sort.Slice(sorted, func(i, j int) bool {
		inum := strings.Split(sorted[i].ScanRows, " ")[0]
		jnum := strings.Split(sorted[j].ScanRows, " ")[0]
		// 将 ScanRows 从字符串转为 int64 进行比较
		valI, _ := strconv.ParseInt(inum, 10, 64)
		valJ, _ := strconv.ParseInt(jnum, 10, 64)
		return valI > valJ // 降序
	})
	return sorted
}

func getConnectionId(appid, stmtid string) (string, util.Process2, error) {
	var result util.Process2
	var feip string
	// 连接到StarRocks
	for _, Feip := range fronendNodes(appid) {
		db, err := conn.StarRocksApp(appid, Feip)
		if err != nil {
			util.Loggrs.Error(err.Error())
			return "", util.Process2{}, err
		}
		////////////////////////////////
		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)
		// 使用context和channel实现优雅终止
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 限制并发数
		sem := make(chan struct{}, 10)
		// 查询进程列表
		var processes []util.Process2
		if err := db.Raw("show full processlist").Scan(&processes).Error; err != nil {
			util.Loggrs.Warn(fmt.Sprintf("failed to get processlist from %s: %v", Feip, err))
			return "", util.Process2{}, err
		}
		for _, p := range processes {
			select {
			case <-ctx.Done():
				// 如果已经找到结果，跳过剩余处理
				break
			case sem <- struct{}{}:
				wg.Add(1)

				go func(p util.Process2) {
					defer func() {
						<-sem
						wg.Done()
					}()

					if p.Id != stmtid {
						return
					}

					mu.Lock()
					result = p
					feip = Feip
					cancel() // 取消所有其他goroutine
					mu.Unlock()
				}(p)
			}
		}
		wg.Wait()
		////////////////////////////////

		/*每次使用完，主动关闭连接数*/
		sqlDB, err := db.DB()
		if err != nil {
			continue
		}
		sqlDB.SetMaxOpenConns(10)                  //最大连接数
		sqlDB.SetMaxIdleConns(10)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(10 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()

	}

	return feip, result, nil
}
