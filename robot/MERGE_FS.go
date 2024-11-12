/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    sessionQueryWarnLark
 *@date    2024/8/8 13:35
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

func SendFsQueris(i *util.InQue, queris bool) (*util.Larkbodys, error) {
	cid := fmt.Sprintf("%d_%s", i.Action, i.Item.Id)
	_, ok := i.FsCache.Get(cid)
	util.Loggrs.Info(fmt.Sprintf("最后关卡：识别缓存中的%s，缓存状态：%t", cid, ok))
	if ok {
		return nil, nil
	}

	edtime, _ := strconv.Atoi(i.Item.Time)

	var SessionSql string
	if len(i.Item.Info) >= 300 {
		SessionSql = i.Item.Info[0:280] + " ..."
	} else {
		SessionSql = i.Item.Info
	}
	SessionSql = strings.NewReplacer("\n", "", `"`, `\"`).Replace(SessionSql)

	var bts string
	if i.Normal {
		bts = "正常"
	} else {
		bts = "倾斜"
	}
	var Queris string
	if queris {
		Queris = "Queries"
	} else {
		Queris = "Process"
	}

	var scan, olaps string
	var count int
	if i.Olapscan != nil {
		if i.Olapscan.OlapScan {
			scan = "全表扫描"
		} else {
			scan = "局部扫描"
		}
		if len(i.Olapscan.OlapPartition) >= 1 {
			olaps = i.Olapscan.OlapPartition[0] + "..."
		}
		count = i.Olapscan.OlapCount
	}

	var color string
	switch i.Action {
	case 0:
		color = "\U0001F7E2"
	case 2:
		color = "\U0001F7E1"
	case 1, 3, 4, 5, 6, 7, 8, 9:
		color = "🔴"
	}

	var msgs []string
	if i.Sign != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Sign:\t\t\t[%s**%s**] (%s)\n`, color, i.Sign, Queris))
	}
	if i.Opinion != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Opinion:\t\t[%s]\n`, i.Opinion))
	}
	if i.App != "" {
		msgs = append(msgs, fmt.Sprintf(`💬App:\t\t\t[%s]\n`, i.App))
	}
	if i.Fe != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Fe:\t\t\t[%s]\n`, i.Fe))
	}
	if i.Item.Host != "" {
		msgs = append(msgs, fmt.Sprintf(`💬ClientIP:\t\t[%s]\n`, i.Item.Host))
	}
	if i.Queris != nil {
		if i.Queris.StartTime != "" {
			msgs = append(msgs, fmt.Sprintf(`💬StartTime:\t\t[%s]\n`, i.Queris.StartTime))
		}
		if i.Queris.QueryId != "" {
			msgs = append(msgs, fmt.Sprintf(`💬QueryId:\t\t[%s]\n`, i.Queris.QueryId))
		}
		if i.Queris.ConnectionId != "" {
			if i.Nature != "" {
				msgs = append(msgs, fmt.Sprintf(`💬ConnectionId:\t[%s] **%s**\n`, i.Queris.ConnectionId, i.Nature))
			} else {
				msgs = append(msgs, fmt.Sprintf(`💬ConnectionId:\t[%s]\n`, i.Queris.ConnectionId))
			}
		}
		if i.Queris.Database != "" {
			msgs = append(msgs, fmt.Sprintf(`💬Database:\t\t[%s]\n`, i.Queris.Database))
		}
		if i.Queris.User != "" {
			msgs = append(msgs, fmt.Sprintf(`💬User:\t\t\t[%s]\n`, i.Queris.User))
		}
		if !strings.Contains(i.Queris.ScanBytes, "0.000") && i.Queris.ScanBytes != "" {
			msgs = append(msgs, fmt.Sprintf(`💬ScanBytes:\t\t[%s]\n`, i.Queris.ScanBytes))
		}
		if i.Queris.ScanRows != "" {
			msgs = append(msgs, fmt.Sprintf(`💬ScanRows:\t\t[%d]\n`, tools.Int64(i.Queris.ScanRows)))
		}
		if !strings.Contains(i.Queris.MemoryUsage, "0.000") && i.Queris.MemoryUsage != "" {
			msgs = append(msgs, fmt.Sprintf(`💬MemoryUsage:\t[%s]\n`, i.Queris.MemoryUsage))
		}
		if !strings.Contains(i.Queris.DiskSpillSize, "0.000") && i.Queris.DiskSpillSize != "" {
			msgs = append(msgs, fmt.Sprintf(`💬DiskSpillSize:\t\t[%s]\n`, i.Queris.DiskSpillSize))
		}
		if !strings.Contains(i.Queris.CPUTime, "0.000") && i.Queris.CPUTime != "" {
			msgs = append(msgs, fmt.Sprintf(`💬CPUTime:\t\t[%s]\n`, tools.GetHour(int(tools.Int64(i.Queris.CPUTime)))))
		}
		if !strings.Contains(i.Queris.ExecTime, "0.000") && i.Queris.ExecTime != "" {
			msgs = append(msgs, fmt.Sprintf(`💬ExecTime:\t\t[**%s**]\n`, tools.GetHour(edtime)))
		}
	} else {
		if i.Item.User != "" {
			msgs = append(msgs, fmt.Sprintf(`💬User:\t\t\t[%s]\n`, i.Item.User))
		}
		if i.Item.Id != "" {
			if i.Nature != "" {
				msgs = append(msgs, fmt.Sprintf(`💬ConnectionId:\t[%s] **%s**\n`, i.Item.Id, i.Nature))
			} else {
				msgs = append(msgs, fmt.Sprintf(`💬ConnectionId:\t[%s]\n`, i.Item.Id))
			}
		}
		if i.Item.Time != "" {
			msgs = append(msgs, fmt.Sprintf(`💬StartTime:\t\t[%s]\n`, time.Now().Add(-time.Second*time.Duration(edtime)).Format("2006-01-02 15:04:05")))
		}
		if count >= 1 {
			msgs = append(msgs, fmt.Sprintf(`💬ScanERows:\t\t[%d]\n`, count))
		}
		if i.Item.Time != "" {
			msgs = append(msgs, fmt.Sprintf(`💬ExecTime:\t\t[**%s**]\n`, tools.GetHour(edtime)))
		}
	}
	if i.Item.Command != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Type:\t\t\t[%s]\n`, i.Item.Command))
	}
	if i.Action == 0 {
		msgs = append(msgs, fmt.Sprintf(`💬State:\t\t\t[**%s**]\n`, i.Item.State))
	} else {
		msgs = append(msgs, fmt.Sprintf(`💬State:\t\t\t[%s]\n`, i.Item.State))
	}
	if len(i.Queryid) >= 1 {
		msgs = append(msgs, fmt.Sprintf(`💬Overlap:\t\t[%d %s ...]\n`, len(i.Queryid), i.Queryid[0]))
	}
	if bts != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Bucket:\t\t\t[%s]\n`, bts))
	}
	if scan != "" {
		msgs = append(msgs, fmt.Sprintf(`💬ScanType:\t\t[%s]\n`, scan))
	}
	if len(i.Iceberg) >= 1 {
		msgs = append(msgs, fmt.Sprintf(`💬Spark:\t\t\t**%s**\n`, i.Iceberg))
	}
	if len(i.Avgs) >= 1 {
		msgs = append(msgs, fmt.Sprintf(`💬Abnormal:\t\t[%s]\n`, strings.Join(i.Avgs, ",")))
	}
	if len(i.Tbs) >= 1 {
		msgs = append(msgs, fmt.Sprintf(`💬Tables:\t\t\t%s\n`, i.Tbs[0]+"..."))
	}
	if olaps != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Explain:\t\t%s\n`, olaps))
	}
	if SessionSql != "" {
		msgs = append(msgs, fmt.Sprintf(`💬Stmt:\t\t\t%v\n`, SessionSql))
	}
	util.Loggrs.Info(fmt.Sprintf("load %s into cache. ", cid))
	i.FsCache.Set(cid, i.Item.Id, cache.DefaultExpiration)
	_, b := i.FsCache.Get(cid)
	util.Loggrs.Info(fmt.Sprintf("%s cache status:%t", cid, b))

	return &util.Larkbodys{
		Message: strings.Join(msgs, ""),
		Logfile: fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, i.Logfile),
	}, nil
}
