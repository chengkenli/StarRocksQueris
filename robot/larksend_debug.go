/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package robot
 *@file    SendFsCart2Info
 *@date    2024/11/13 13:59
 */

package robot

import (
	"StarRocksQueris/util"
	"fmt"
	"strings"
)

// SendFsCart2Debug global info日志往这里推送
func SendFsCart2Debug(msgs []string) {
	if util.Config.GetString("mode.Debug") == "" {
		return
	}
	var title string
	if len(msgs) >= 10 {
		title = fmt.Sprintf("[G]慢查询告警(%d) 告警过多，高度关注查询队列是否堵塞！", len(msgs))
	} else {
		title = fmt.Sprintf("[G]慢查询告警(%d)", len(msgs))
	}

	msg := fmt.Sprintf(`{
    "msg_type": "interactive",
    "card": {
        "elements": [
            %s
        ],
        "header": {
            "template": "%s",
            "title": {
                "content": "%s",
                "tag": "plain_text"
            }
        }
    }
}`, textDebug(strings.Join(msgs, ",")), "wathet", title)

	r := SendFsPost("POST", fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", util.Config.GetString("mode.Debug")), strings.NewReader(msg))
	if strings.Contains(string(r), "Bad Request") || strings.Contains(string(r), "err") {
		util.Loggrs.Error(uid, msg)
	} else {
		util.Loggrs.Info(uid, msg)
	}
	util.Loggrs.Info(uid, string(r))
}
