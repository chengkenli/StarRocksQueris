/*
 *@author  chengkenli
 *@project StarRocksRM
 *@package app
 *@file    msg
 *@date    2024/11/20 17:27
 */

package short

import (
	"StarRocksQueris/util"
	"fmt"
	"strings"
	"time"
)

func Body(p *Portc) *util.Larkbodys {
	var sgin, ch string
	switch p.State {
	case -1:
		sgin = `🔷`
		ch = "SLEEP"
	case 0:
		sgin = `🟠`
		ch = "CLOSE"
	case 1:
		sgin = `🟢`
		ch = "OPEN"
	case 2:
		sgin = `🟦`
		ch = "ON GOING"
	}

	ago := time.Now().Add(-2 * time.Hour).Format("15:04")
	neo := time.Now().Format("15:04")

	var msga []string

	if p.App != "" {
		msga = append(msga, fmt.Sprintf(`[集群实例]：[%s]\n`, p.App))
	}
	if p.Timetr != "" {
		msga = append(msga, fmt.Sprintf(`[维护时间]：[%s]\n`, p.SrcData.Ctime))
	}
	if strings.Contains(p.SrcData.Ctime, ",") {
		msga = append(msga, fmt.Sprintf(`[维护阶段]：[%s]\n`, p.Timetr))
	}
	msga = append(msga, fmt.Sprintf(`[采集范围]：[%s-%s]\n`, ago, neo))

	if len(p.Core) != 0 {
		msga = append(msga, fmt.Sprintf(`[保底资源]：[%s]\n`, p.Core[0]))
	}

	if p.ProceVal > 0 {
		msga = append(msga, fmt.Sprintf(`[维护进度]：[%s] (%.2f%%)\n`, p.ProceBar, p.ProceVal))
	}

	msgs := fmt.Sprintf(`[查询保障]：[%s]\n[触发时间]：[%s]\n[保障状态]：[%s]\n%s\n%s\n%s`,
		sgin,
		time.Now().Format("2006-01-02 15:04:05"),
		ch,
		strings.Join(msga, ""),
		p.Comment,
		strings.Join(p.Resource, ""),
	)
	loggrs.Info(uid, msgs)
	return &util.Larkbodys{
		App:     p.App,
		Message: msgs,
		Logfile: fmt.Sprintf("http://%s:7890/html%s", util.H.Ip, p.Logfile),
		Action:  p.State,
	}
}
