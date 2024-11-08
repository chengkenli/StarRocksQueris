/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendQueris
 *@date    2024/9/5 11:44
 */

package fronends

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/antchfx/htmlquery"
	"github.com/go-resty/resty/v2"
	"golang.org/x/net/html"
	"log"
	"os"
	"strconv"
	"strings"
)

func UriCurrentQueries(app, fe string) util.Queris {
	// 登录FE，8030端口的账号密码（管理员）
	username, password := authLogin(app)

	var qs util.Queris
	uri := fmt.Sprintf(`http://%s:8030/system?path=//current_queries`, fe)
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	log.New(os.Stdout, "", log.LstdFlags)
	//发送POST请求并处理响应
	respones, err := Client.R().SetBasicAuth(username, password).Get(uri)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	menu, _ := htmlquery.Parse(strings.NewReader(string(respones.Body())))
	table := htmlquery.Find(menu, `//*[@id="table_id"]/tbody/tr`)
	for _, node := range table {
		tr := htmlquery.Find(node, "td")
		if len(tr) == 12 {
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
				Warehouse:     td(tr[11]),
			})
		}
	}
	return qs
}

func UriCurrentQueriesStmt(app, fe, id string) string {
	// 登录FE，8030端口的账号密码（管理员）
	username, password := authLogin(app)

	uri := fmt.Sprintf(`http://%s:8030/system?path=//current_queries/%s`, fe, id)
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应
	respones, err := Client.R().SetBasicAuth(username, password).Get(uri)
	if err != nil {
		util.Loggrs.Error(err)
		return ""
	}
	menu, _ := htmlquery.Parse(strings.NewReader(string(respones.Body())))
	var sql *html.Node
	if menu != nil {
		sql = htmlquery.FindOne(menu, `//*[@id="table_id"]/tbody/tr/td/a`)
	}
	var stmt string
	if sql != nil {
		stmt = htmlquery.InnerText(sql)
	}
	return stmt
}

func UriCurrentQueriesHosts(app, fe, id string) []string {
	// 登录FE，8030端口的账号密码（管理员）
	username, password := authLogin(app)

	var nodes []string
	uri := fmt.Sprintf(`http://%s:8030/system?path=//current_queries/%s/hosts`, fe, id)
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应
	respones, err := Client.R().SetBasicAuth(username, password).Get(uri)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	nodes = append(nodes, fmt.Sprintf("  %-2s %-20s %-15s %-15s %-15s %-15s", "ID", "Host", "ScanBytes", "ScanRows", "CpuCostSeconds", "MemUsageBytes"))

	menu, _ := htmlquery.Parse(strings.NewReader(string(respones.Body())))
	tbody := htmlquery.Find(menu, `//*[@id="table_id"]/tbody/tr`)
	if tbody != nil {
		for i, body := range tbody {
			if body == nil {
				continue
			}
			tr := htmlquery.Find(body, "td")
			if len(tr) == 5 {
				msg := fmt.Sprintf("> %-2d %-20s %-15s %-15s %-15s %-15s ", i, td(tr[0]), td(tr[1]), td(tr[2]), td(tr[3]), td(tr[4]))
				nodes = append(nodes, msg)
			}
		}
	}
	return nodes
}

func td(n *html.Node) string {
	v := htmlquery.InnerText(n)
	return v
}

func UriCQHIP(app, fe, queryid string) []string {
	// 登录FE，8030端口的账号密码（管理员）
	username, password := authLogin(app)

	var nodes []string
	uri := fmt.Sprintf(`http://%s:8030/system?path=//current_queries/%s/hosts`, fe, queryid)
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应
	respones, err := Client.R().SetBasicAuth(username, password).Get(uri)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	menu, _ := htmlquery.Parse(strings.NewReader(string(respones.Body())))
	tbody := htmlquery.Find(menu, `//*[@id="table_id"]/tbody/tr`)
	if tbody != nil {
		for _, body := range tbody {
			if body == nil {
				continue
			}
			tr := htmlquery.Find(body, "td")
			if len(tr) == 5 {
				msg := fmt.Sprintf("> %s, %s, %s, %s, %s", td(tr[0]), td(tr[1]), td(tr[2]), td(tr[3]), td(tr[4]))

				util.Loggrs.Info(fmt.Sprintf("##[检查] %s %s %v", fe, queryid, msg))
				//MemUsageBytes
				memBytes := td(tr[4])
				if !strings.Contains(strings.ToLower(memBytes), " gb") {
					continue
				}
				mu := strings.Split(memBytes, " ")
				if len(mu) < 2 {
					continue
				}
				memoryUsage, _ := strconv.ParseFloat(mu[0], 64)
				if memoryUsage < 200 {
					continue
				}
				nodes = append(nodes, msg)
				util.Loggrs.Info(fmt.Sprintf("##[命中] %s %s %v", fe, queryid, msg))
			}
		}
	}
	return nodes
}

// 登录FE，8030端口的账号密码（管理员）
func authLogin(app string) (string, string) {
	// 登录FE，8030端口的账号密码（管理员）
	var username, password string
	for _, m := range util.ConnectBody {
		if m["app"].(string) == app {
			username = m["user"].(string)
			password = m["password"].(string)
		}
	}
	return username, password
}
