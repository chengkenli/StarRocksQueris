/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package robot
 *@file    SendFsCart2Warn
 *@date    2024/11/13 14:00
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"strings"
	"sync"
)

// SendFsCart2Warn global warn日志往这里推送
func SendFsCart2Warn(msgs []string) {
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

	ch := make(chan struct{}, 2)
	var wg sync.WaitGroup

	for _, m := range tools.UniqueMaps(util.ConnectRobot) {
		wg.Add(1)
		go func(m map[string]interface{}) {
			defer func() {
				<-ch
				wg.Done()
			}()

			ch <- struct{}{}

			if m["type"] != nil {
				if m["type"].(string) != "global" {
					return
				}
				if m["robot"] == nil {
					return
				}
				robot := m["robot"].(string)

				r := SendFsPost("POST", fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", robot), strings.NewReader(msg))
				if strings.Contains(string(r), "Bad Request") || strings.Contains(string(r), "err") {
					util.Loggrs.Error(uid, msg)
				} else {
					util.Loggrs.Info(uid, msg)
				}
				util.Loggrs.Info(uid, string(r))
			}
		}(m)
	}
	wg.Wait()
}
