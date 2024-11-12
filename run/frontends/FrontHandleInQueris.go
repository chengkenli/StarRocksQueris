/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleInQueris
 *@date    2024/9/4 9:34
 */

package fronends

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func InQueris(i *util.InQue) (*util.Larkbodys, *util.SchemaData) {
	nt := time.Now()
	defer func() {
		util.Loggrs.Info(fmt.Sprintf("分析队列信息 %s %v", i.Item.Id, time.Now().Sub(nt).String()))
	}()
	// end
	go QuerusFile(i)
	// 发送飞书提醒
	body, err := robot.SendFsQueris(i, true)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return body, nil
	}
	// 组合落表数据
	t64, _ := strconv.ParseInt(i.Item.Time, 10, 64)
	//判断行为
	// 从表中获取主题域与相关owner、邮件人
	var info *util.EmailMain
	var owner, domain string
	if i.Schema != nil {
		info = SessionSchemaRegexpOwner(0, i.Schema)
		if len(info.EmailTo) != 0 {
			if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
				owner = strings.ReplaceAll(info.EmailTo[0], util.ConnectNorm.SlowQueryEmailSuffix, "")
			}
		}
		if len(info.Domain) != 0 {
			domain = strings.Join(info.Domain, ",")
		}
	}

	//生成map
	sdata := &util.SchemaData{
		Ts:                time.Now().Format("2006-01-02"),
		App:               i.App,
		QueryId:           i.Item.Id,
		Origin:            strings.Join(i.Tbs, ","),
		Domain:            domain,
		Owner:             owner,
		Action:            i.Action,
		Timestamp:         i.Queris.StartTime,
		QueryType:         i.Item.Command,
		ClientIp:          i.Item.Host,
		User:              i.Item.User,
		AuthorizedUser:    "",
		ResourceGroup:     "",
		Catalog:           "",
		Db:                i.Item.Db,
		State:             i.Item.State,
		ErrorCode:         "",
		QueryTime:         t64,
		ScanBytes:         Int64(i.Queris.ScanBytes) * 1024 * 1024 * 1024,
		ScanRows:          Int64(i.Queris.ScanRows),
		ReturnRows:        0,
		CpuCostNs:         Int64(i.Queris.CPUTime),
		MemCostBytes:      Int64(i.Queris.MemoryUsage) * 1024 * 1024 * 1024,
		StmtId:            0,
		IsQuery:           0,
		FeIp:              "",
		Stmt:              i.Item.Info,
		Digest:            "",
		PlanCpuCosts:      0,
		PlanMemCosts:      0,
		PendingTimeMs:     0,
		Logfile:           fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, i.Logfile),
		Optimization:      0,
		OptimizationItems: "",
	}

	// 发送邮件提醒
	go func() {
		_, Ok := i.EmCache.Get(fmt.Sprintf("email_%s", i.Item.Id))
		if Ok {
			return
		}
		robot.SendEmQueris(
			&robot.WarnQuerisEmail{
				Queris: i,
				Avgs:   i.Avgs,
			})
	}()
	return body, sdata
}

func Int64(str string) int64 {
	parseInt, err := strconv.ParseFloat(strings.NewReplacer(" ", "").Replace(strings.Split(str, " ")[0]), 64)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return int64(parseInt)
	}
	return int64(parseInt)
}
