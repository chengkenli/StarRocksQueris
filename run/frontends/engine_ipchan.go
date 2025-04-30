/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package clientip
 *@file    RunClientIP_Chan
 *@date    2025/2/5 10:50
 */

package fronends

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"net"
	"strings"
	"time"
)

func (w *Workers) clientChan(item *util.Process2) {
	if util.P.Check {
		return
	}
	if item == nil {
		return
	}
	ip := strings.Split(item.Host, ":")
	if len(ip) < 2 {
		return
	}
	n, _ := net.LookupAddr(ip[0])
	var hostname string
	if n != nil {
		hostname = n[0]
	}
	h1 := strings.ToLower(strings.Split(hostname, ".")[0])

	//判断IP数据是否已经存在，如已经存在，则跳过
	if tools.StringInSlice(ip[0], util.ClientDatas) {
		return
	}

	if strings.Contains(h1, "oser") ||
		strings.Contains(h1, "pcnnt") ||
		strings.Contains(h1, "tstr") ||
		strings.Contains(h1, "-") ||
		strings.Contains(h1, "cerr") {
		return
	}

	uid := xid.Xid(&xid.Uid{
		App:  "",
		Fe:   item.Host,
		Mode: "client",
		Id:   item.Id,
	})

	//var i int
	for _, data := range util.ClientIPDec {
		h2 := strings.ToLower(strings.Split(data.SystemName, ".")[0])
		if h1 == h2 && len(data.User) != 0 {
			util.Loggrs.Info(uid, fmt.Sprintf("%s 这个IP已经依赖了数据表中的 %s，不进行导入, 解析出来的:<%s>,表数据中的:<%s>", ip[0], data.SystemName, h1, h2))
			return
		}
	}
	//
	//if i == 10 {
	//	return
	//}

	util.Loggrs.Info(uid, fmt.Sprintf("%s 这个IP没有找到任何依赖, 物理地址:%s, 需要导入.", ip[0], hostname))
	// 从这里开始，将IP地址信息进行落表
	w.clientDatas <- &util.ClientIPData{
		Ip:              ip[0],
		User:            "",
		SystemName:      hostname,
		ConsoleUser:     "",
		Manufacturer:    "",
		Model:           "",
		OperatingSystem: "",
		Timestamp:       time.Now().Format("2006-01-02 15:04:05"),
	}
}
