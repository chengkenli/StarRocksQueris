/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    frontendQueryToFile
 *@date    2024/8/8 14:11
 */

package pipe

import (
	"StarRocksQueris/etrics"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func QuerusFile(i *util.InQue) {
	// 资源隔离管控(仅用户集群)
	if i.App == "sr-adhoc" {
		go etrics.ResourceGroup(i.Connect, i.Item.User)
	}

	edtime, _ := strconv.Atoi(i.Item.Time)

	var sk []string
	for _, k := range i.Sortkey {
		sk = append(sk, k.Schema,
			fmt.Sprintf("%-5s: %s", "当前序列", strings.Join(k.SortKey.SplikKey, "、")),
			fmt.Sprintf("%-5s: %s", "当前排序", strings.Join(k.SortKey.SortKey, "、")),
			fmt.Sprintf("%-5s: %s", "最佳排序", strings.Join(k.SortKey.SplitKeys, "、")),
			fmt.Sprintf("%-7s: %s", "数值", strings.Join(k.SortKey.IntArr, "、")),
			fmt.Sprintf("%-6s: %s", "小数位", strings.Join(k.SortKey.DecArr, "、")),
			fmt.Sprintf("%-7s: %s", "日期", strings.Join(k.SortKey.DateArr, "、")),
			fmt.Sprintf("%-6s: %s", "字符串", strings.Join(k.SortKey.StrArr, "、")),
		)
		sk = append(sk, "\n")
	}
	var olaps string
	if i.Olapscan != nil {
		olaps = strings.Join(i.Olapscan.OlapPartition, "\n")
	}

	var msg string
	if i.Queris == nil {
		msg = fmt.Sprintf(`
💬App:            %s
💬Fe:             %s
💬ClientIP:       %s
💬Type:           %s
💬Overlap:        %s
💬Bucket:         %t
💬StartTime:      %s
💬QueryId:        %s
💬ConnectionId:   %s
💬Database:       %s
💬User:           %s
💬ScanType:       %t
💬ScanBytes:      %s
💬ScanRows:       %s
💬MemoryUsage:    %s
💬DiskSpillSize:  %s
💬CPUTime:        %s
💬ExecTime:       %s
💬Tables:         %s
💬Nodes:          
%s
💬Explain:        
%s
💬Stmt:           
%s
💬Replica:
%s
💬SortKey:
%s
💬Buckets:
%s`, i.App, i.Fe, i.Item.Host, i.Item.Command, i.Queryid, i.Normal,
			time.Now().Add(-time.Second*time.Duration(edtime)).Format("2006-01-02 15:04:05"),
			i.Item.Id,
			i.Item.Id,
			i.Item.Db,
			i.Item.User,
			i.Normal,
			"",
			"",
			"",
			"",
			"",
			tools.GetHour(edtime),
			strings.Join(i.Tbs, ","),
			"",
			olaps,
			i.Item.Info,
			strings.Join(i.Rd, "\n"),
			strings.Join(sk, "\n"),
			strings.Join(i.Buckets, "\n"),
		)
	} else {

		msg = fmt.Sprintf(`
💬App:            %s
💬Fe:             %s
💬ClientIP:       %s
💬Type:           %s
💬Overlap:        %s
💬Bucket:         %t
💬StartTime:      %s
💬QueryId:        %s
💬ConnectionId:   %s
💬Database:       %s
💬User:           %s
💬ScanType:       %t
💬ScanBytes:      %s
💬ScanRows:       %s
💬MemoryUsage:    %s
💬DiskSpillSize:  %s
💬CPUTime:        %s
💬ExecTime:       %s
💬Tables:         %s
💬Nodes:          
%s
💬Explain:        
%s
💬Stmt:           
%s
💬Replica:
%s
💬SortKey:
%s
💬Buckets:
%s`, i.App, i.Fe, i.Item.Host, i.Item.Command, i.Queryid, i.Normal,
			i.Queris.StartTime,
			i.Queris.QueryId,
			i.Queris.ConnectionId,
			i.Queris.Database,
			i.Queris.User,
			i.Normal,
			i.Queris.ScanBytes,
			i.Queris.ScanRows,
			i.Queris.MemoryUsage,
			i.Queris.DiskSpillSize,
			i.Queris.CPUTime,
			i.Queris.ExecTime,
			strings.Join(i.Tbs, ","),
			strings.Join(i.Queris.Nodes, "\n"),
			olaps,
			i.Item.Info,
			strings.Join(i.Rd, "\n"),
			strings.Join(sk, "\n"),
			strings.Join(i.Buckets, "\n"),
		)
	}

	tools.WriteFile(i.Logfile, msg)
}
