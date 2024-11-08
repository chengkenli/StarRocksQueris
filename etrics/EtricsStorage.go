package etrics

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

/*Storage 发送集群最高存储通知*/
func storage(app string) *util.Larkbodys {
	db, err := conn.StarRocks(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}

	var messages []string
	var b util.Backends
	r := db.Raw("show backends").Scan(&b)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return nil
	}

	logfile := fmt.Sprintf("%s/%s_%d.sql", util.LogPath, app, time.Now().UnixNano())
	var f, g, k float64
	var maxPct []float64
	for _, info := range b {
		f = f + flos(info.TotalCapacity)
		k = k + flos(info.DataUsedCapacity)
		g = g + flos(info.AvailCapacity)
		if flos(info.MaxDiskUsedPct) >= vas {
			node := fmt.Sprintf("IP:%s\tDataUsedCapacity:%s\tUsedPct:%s\tMaxDiskUsedPct:%s\tAvailCapacity:%s\tTotalCapacity:%s\n",
				info.IP, sl(info.DataUsedCapacity), sl(info.UsedPct), sl(info.MaxDiskUsedPct), sl(info.AvailCapacity), sl(info.TotalCapacity),
			)
			messages = append(messages, node)
			tools.WriteFile(logfile, node)
			maxPct = append(maxPct, flos(info.MaxDiskUsedPct))
		}
	}

	if len(messages) == 0 {
		return nil
	}
	tools.WriteFile(logfile, "\n")
	//h := f - g
	//tools.WriteFile(logfile, fmt.Sprintf("%s总存储:%0.2ftb, 目前存储:%0.2ftb(数据实际:%0.2ftb), 空闲:%0.2ftb, 百分比:%0.2f%%", app, f, h, k, f-h, h/f*100))
	StCache.Set(app, maxPct, cache.DefaultExpiration)
	util.Loggrs.Info(fmt.Sprintf("节点存储预警 ==> %s 集群存储告警，加入定时缓存机制！", app))

	return &util.Larkbodys{
		Message: fmt.Sprintf("集群:[%s]，存储达到:[**%0.2f%%**]，涉及节点数:[%d].", app, tools.MaxFloat64(maxPct), len(messages)),
		Logfile: fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, logfile),
	}
}

func sl(s string) string {
	return strings.ReplaceAll(s, " ", "")
}

func flos(s string) float64 {
	var maxDiskUsedPct float64
	if strings.Contains(strings.ToLower(sl(s)), "gb") {
		m := strings.Split(s, " ")[0]
		maxDiskUsedPct, _ = strconv.ParseFloat(m, 64)
		maxDiskUsedPct = maxDiskUsedPct / 1024
		return maxDiskUsedPct
	}
	m := strings.Split(s, " ")[0]
	maxDiskUsedPct, _ = strconv.ParseFloat(m, 64)
	return maxDiskUsedPct
}
