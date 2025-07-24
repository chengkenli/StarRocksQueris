/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    engine_queue
 *@date    2025/5/9 13:19
 */

package pipe

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/antchfx/htmlquery"
	"github.com/go-resty/resty/v2"
	"gorm.io/gorm"
	"strings"
)

func queues(db *gorm.DB, app, fe string) util.Queris {
	var a tools.SrAvgs
	//匹配元数据
	for _, m := range tools.UniqueMaps(util.ConnectBody) {
		if m["app"].(string) == app {
			a = tools.SrAvgs{
				Host: m["feip"].(string),
				Port: int(m["feport"].(int32)),
				User: m["user"].(string),
				Pass: m["password"].(string),
			}
		}
	}
	//创建Resty客户端
	client := resty.New().SetLogger(&util.CustomLogger{}).SetBasicAuth(a.User, a.Pass)

	var qs util.Queris
	uri := fmt.Sprintf(`http://%s:8030/system?path=//current_queries`, fe)
	//创建Resty客户端
	//发送POST请求并处理响应
	respones, err := client.R().Get(uri)
	if err != nil {
		return nil
	}
	menu, _ := htmlquery.Parse(strings.NewReader(string(respones.Body())))
	table := htmlquery.Find(menu, `//*[@id="table_id"]/tbody/tr`)
	for _, node := range table {
		tr := htmlquery.Find(node, "td")
		if len(tr) >= 11 {
			var wh string
			if len(tr) == 12 {
				wh = td(tr[11])
			}

			if tools.Version(db) >= 3.3 {
				qs = append(qs, util.Querisign{
					StartTime:     td(tr[0]),
					QueryId:       td(tr[2]),
					ConnectionId:  td(tr[3]),
					Database:      td(tr[4]),
					User:          td(tr[5]),
					ScanBytes:     td(tr[6]),
					ScanRows:      td(tr[7]),
					MemoryUsage:   td(tr[8]),
					DiskSpillSize: td(tr[9]),
					CPUTime:       td(tr[10]),
					ExecTime:      td(tr[11]),
					Warehouse:     wh,
				})
			} else {
				qs = append(qs, util.Querisign{
					StartTime:     td(tr[0]),
					QueryId:       td(tr[1]),
					ConnectionId:  td(tr[2]),
					Database:      td(tr[3]),
					User:          td(tr[4]),
					ScanBytes:     td(tr[5]),
					ScanRows:      td(tr[6]),
					MemoryUsage:   td(tr[7]),
					DiskSpillSize: td(tr[8]),
					CPUTime:       td(tr[9]),
					ExecTime:      td(tr[10]),
					Warehouse:     wh,
				})
			}
		}
	}
	return qs
}
