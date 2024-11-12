/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendGrafana
 *@date    2024/9/24 16:19
 */

package fronends

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/go-resty/resty/v2"
)

// FrontGrafana 向监控api汇报数据
func FrontGrafana(g *util.Grafana) {

	if len(util.ConnectNorm.SlowQueryGrafana) == 0 {
		return
	}
	if util.P.Check {
		return
	}

	var app string
	switch g.App {
	case "sr-adhoc":
		app = "tx_sr_adhoc"
	case "sr-app":
		app = "tx_sr_app"
	case "sccts":
		app = "lc_sr_scct_new"
	case "cdp":
		app = "lc_sr_cdp"
	case "api":
		app = "lc_sr_cdp_api"
	case "ma":
		app = "lc_sr_ma"
	}
	if app == "" {
		util.Loggrs.Warn(app + " is nil.")
		return
	}

	body := fmt.Sprintf(`
{
    "values": [
        {
            "value": 1,
            "tags": {
                "cluster": "%s",
                "action": "%s",
                "connectionid": "%s",
                "user": "%s"
            }
        }
    ],
    "app": "sr",
    "token": "e2e99b51",
    "metric": "slow_query"
}`, app, g.Sign, g.ConnectionId, g.User)
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应
	response, err := Client.R().SetHeader("Content-Type", "application/json").SetBody(body).Post(util.ConnectNorm.SlowQueryGrafana)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	util.Loggrs.Info(string(response.Body()))
}
