/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    SendFsCartID
 *@date    2024/9/13 16:43
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// SendFsCartService2Group session 通过[服务账号]分发到不同的群组
func SendFsCartService2Group(body []*util.Larkbodys) {
	if len(tools.UniqueMaps(util.ConnectRobot)) == 0 {
		return
	}

	for _, m := range tools.UniqueMaps(util.ConnectRobot) {
		if m["type"] != nil {
			if m["type"].(string) != "cluster" {
				continue
			}
			if m["key"] == nil {
				continue
			}
			if m["robot"] == nil {
				continue
			}

			app := m["key"].(string)
			robot := m["robot"].(string)

			var msgs []string
			for i, larkbodys := range body {
				if len(larkbodys.Message) == 0 {
					continue
				}
				if !strings.Contains(larkbodys.Message, fmt.Sprintf("[%s]", app)) {
					continue
				}

				message := larkbodys.Message
				apps := regexp.MustCompile(`App:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "").Replace(message))
				user := regexp.MustCompile(`User:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "").Replace(message))
				if len(user) < 1 {
					continue
				}
				if app != apps[1] {
					continue
				}
				if app != user[1] {
					continue
				}

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

			if msgs == nil {
				continue
			}

			util.Loggrs.Info(fmt.Sprintf("通过[服务账号]分发到不同的群组"))

			var title string
			if len(msgs) >= 10 {
				title = fmt.Sprintf("[I]慢查询告警(%d) 告警过多，高度关注查询队列是否堵塞！", len(msgs))
			} else {
				title = fmt.Sprintf("[I]慢查询告警(%d)", len(msgs))
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
			for _, roboot := range strings.Split(robot, ",") {
				if roboot == "" {
					continue
				}
				wg.Add(1)
				go func(roboot string) {
					defer func() {
						<-ch
						wg.Done()
					}()

					ch <- struct{}{}
					r := SendFsPost("POST", fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", roboot), strings.NewReader(msg))
					if strings.Contains(string(r), "Bad Request") || strings.Contains(string(r), "err") {
						util.Loggrs.Error(msg)
					}
					util.Loggrs.Info(fmt.Sprintf("feishu:[%s] %s", roboot, string(r)))
				}(roboot)
			}
			wg.Wait()
		}
	}
}
