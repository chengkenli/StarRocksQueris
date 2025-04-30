/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_count
 *@date    2024/11/21 15:31
 */

package short

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"time"
)

func sumc(db *gorm.DB, user string) (int, int, int, int, []HtmlData) {
	if util.ConnectNorm.SlowQueryAuditload == "" {
		return -1, -1, -1, -1, nil
	}

	ago := time.Now().Add(-2 * time.Hour).Format("2006-01-02 15:04:05")

	var sumdata []map[string]interface{}
	sql := fmt.Sprintf("select user,queryId,timestamp,queryTime,scanBytes,scanRows,returnRows,cpuCostNs,memCostBytes,stmt from %s where timestamp>='%s' and user='%s' and clientIp not like '%%%s%%' and lower(stmt) like '%%select%%' order by queryTime desc", util.ConnectNorm.SlowQueryAuditload, ago, user, util.H.Ip)
	x := db.Raw(sql).Scan(&sumdata)
	if x.Error != nil {
		loggrs.Error(uid, x.Error.Error())
		return -1, -1, -1, -1, nil
	}

	var millis, second, minute []string
	//ds = append(ds, user)

	var htmldata []HtmlData
	for i, c := range sumdata {
		if i < 10 {
			//marshal, _ := json.Marshal(&c)
			//ds = append(ds, fmt.Sprintf("#%d %v", i, string(marshal)))
			if _, ok := last5min.Get(user); !ok {
				stmtFile := fmt.Sprintf("%s/%s.log", util.Config.GetString("logger.LogPath"), c["queryId"].(string))
				go tools.WriteFile(stmtFile, c["stmt"].(string))

				htmldata = append(htmldata, HtmlData{
					Id:           i,
					User:         c["user"].(string),
					CpuCostNs:    c["cpuCostNs"].(int64),
					MemCostBytes: c["memCostBytes"].(int64),
					QueryId:      c["queryId"].(string),
					QueryTime:    c["queryTime"].(int64),
					ReturnRows:   c["returnRows"].(int64),
					ScanBytes:    c["scanBytes"].(int64),
					ScanRows:     c["scanRows"].(int64),
					Timestamp:    c["timestamp"].(time.Time).Format("2006-01-02 15:04:05"),
					Stmt:         fmt.Sprintf("http://%s:7890/log%s", util.H.Ip, stmtFile),
				})
			}
		}
		// 毫秒
		if c["queryTime"].(int64) < 1000 {
			millis = append(millis, c["queryId"].(string))
			continue
		}
		// 秒
		if c["queryTime"].(int64)/1000 >= 1 && c["queryTime"].(int64)/1000 < 60 {
			second = append(second, c["queryId"].(string))
			continue
		}
		// 分钟
		if c["queryTime"].(int64)/1000 > 60 {
			minute = append(minute, c["queryId"].(string))
			continue
		}
	}
	//ds = append(ds, "\n")

	return len(sumdata), len(millis), len(second), len(minute), htmldata

}
