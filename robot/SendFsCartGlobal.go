/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    SendFsCartGlobal
 *@date    2024/9/13 16:43
 */

package robot

import (
	"StarRocksQueris/util"
	"fmt"
	"strings"
	"sync"
)

// SendFsCartGlobal global
func SendFsCartGlobal(body []*util.Larkbodys) {
	var msgs []string
	for i, larkbodys := range body {
		msgs = append(msgs, fmt.Sprintf(`
{
                "tag": "div",
                "text": {
                    "content": "#%d\n%s",
                    "tag": "lark_md"
                }
            },
            {
                "actions": [
                    {
                        "tag": "button",
                        "text": {
                            "content": "日志",
                            "tag": "lark_md"
                        },
                        "url": "%s",
                        "type": "default",
                        "value": {
                            
                        }
                    }
                ],
                "tag": "action"
            }`, i, larkbodys.Message, larkbodys.Logfile))
	}

	var title string
	if len(msgs) >= 10 {
		title = fmt.Sprintf("[G]慢查询告警(%d) 告警过多，高度关注查询队列是否堵塞！", len(msgs))
	} else {
		title = fmt.Sprintf("[G]慢查询告警(%d)", len(msgs))
	}

	msg := fmt.Sprintf(`
{
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

	for _, m := range util.ConnectRobot {

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
					util.Loggrs.Error(msg)
				}
				util.Loggrs.Info(string(r))
			}
		}(m)
	}
	wg.Wait()
}
