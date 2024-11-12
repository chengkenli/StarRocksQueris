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
func Storage(app string) *util.Larkbodys {
	vas := 75

	util.Loggrs.Info("存储扫描.")
	db, err := conn.StarRocks(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}

	var amsgs []string
	var b util.Backends
	r := db.Raw("show backends").Scan(&b)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return nil
	}

	logfile := fmt.Sprintf("%s/%s_%d.sql", util.LogPath, app, time.Now().UnixNano())
	//url := fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, logfile)
	var f, g, k float64
	var maxPct []float64
	for _, info := range b {
		f = f + flos(info.TotalCapacity)
		k = k + flos(info.DataUsedCapacity)
		g = g + flos(info.AvailCapacity)
		if flos(info.MaxDiskUsedPct) >= float64(vas) {
			node := fmt.Sprintf("IP:%s\tDataUsedCapacity:%s\tUsedPct:%s\tMaxDiskUsedPct:%s\tAvailCapacity:%s\tTotalCapacity:%s\n",
				info.IP, sl(info.DataUsedCapacity), sl(info.UsedPct), sl(info.MaxDiskUsedPct), sl(info.AvailCapacity), sl(info.TotalCapacity),
			)
			amsgs = append(amsgs, fmt.Sprintf(`🟡- 告警节点：[%s]，总存储：[%s]，目前使用率：[%s]\n`, info.IP, sl(info.TotalCapacity), sl(info.MaxDiskUsedPct)))
			tools.WriteFile(logfile, node)
			maxPct = append(maxPct, flos(info.MaxDiskUsedPct))
		}
	}

	if len(amsgs) == 0 {
		return nil
	}
	tools.WriteFile(logfile, "\n")
	//h := f - g
	//tools.WriteFile(logfile, fmt.Sprintf("%s总存储:%0.2ftb, 目前存储:%0.2ftb(数据实际:%0.2ftb), 空闲:%0.2ftb, 百分比:%0.2f%%", app, f, h, k, f-h, h/f*100))
	StCache.Set(app, maxPct, cache.DefaultExpiration)
	util.Loggrs.Info(fmt.Sprintf("节点存储预警 ==> %s 集群存储告警，加入定时缓存机制！", app))

	// 重新定义发送内容
	var sgin string
	if tools.MaxFloat64(maxPct) >= float64(vas) && tools.MaxFloat64(maxPct) < 80 {
		sgin = `🟡`
	} else if tools.MaxFloat64(maxPct) >= 80 && tools.MaxFloat64(maxPct) < 85 {
		sgin = `🟠`
	} else if tools.MaxFloat64(maxPct) >= 85 {
		sgin = `🔴`
	}

	msgs := fmt.Sprintf(`[告警级别]：[%s]\n[告警时间]：[%s]\n[集群实例]：[%s]\n[告警内容]：\n您好！系统监测到集群使用率最高已经达到 [%.2f%%]，涉及告警的节点有 [%d] 个，(可点击下面的log按钮进行查看) 具体如下：\n%s`,
		sgin,
		time.Now().Format("2006-01-02 15:04:05"),
		app,
		tools.MaxFloat64(maxPct),
		len(amsgs),
		strings.Join(amsgs, ""),
	)
	util.Loggrs.Info(msgs)
	return &util.Larkbodys{
		Message: msgs,
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
