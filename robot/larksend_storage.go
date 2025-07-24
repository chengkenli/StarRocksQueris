/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    SendFsCartGlobal
 *@date    2024/9/13 16:43
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strings"
	"sync"
)

// SendFsCartStorage global
func SendFsCartStorage(body []*util.Larkbodys) {
	go SendFsCartStorageGlobal(body)

	//if len(tools.UniqueMaps(util.ConnectRobot)) == 0 {
	//	return
	//}
	// 获取机器人
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

			robotCache.Set(app, robot, cache.DefaultExpiration)
		}
	}

	var msgs []string
	for i, larkbodys := range body {
		util.Loggrs.Info(uid, larkbodys.App)
		if larkbodys == nil {
			util.Loggrs.Warn(uid, "主体为空")
			continue
		}
		if len(larkbodys.Message) == 0 {
			util.Loggrs.Warn(uid, "消息为空")
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

		if larkbodys.App == "sr-adhoc" {
			robotCache.Set("sr-adhoc", "904fe377-c76c-4184-8e35-cc471de283c6", cache.DefaultExpiration)
		}

		if msgs == nil {
			continue
		}
		util.Loggrs.Info(uid, fmt.Sprintf("通过[集群名称]分发给不同的群组"))

		var title string
		if len(msgs) >= 10 {
			title = fmt.Sprintf("[S]存储告警(%d) 告警过多，高度关注集群存储状态是否正常！", len(msgs))
		} else {
			title = fmt.Sprintf("[S]存储告警(%d)", len(msgs))
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

		if v, ok := robotCache.Get(larkbodys.App); ok {
			ch := make(chan struct{}, 2)
			var wg sync.WaitGroup
			for _, roboot := range strings.Split(v.(string), ",") {
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
						util.Loggrs.Error(uid, msg)
					}
					util.Loggrs.Info(uid, fmt.Sprintf("feishu:[%s] %s", roboot, string(r)))
				}(roboot)
			}
			wg.Wait()
		}
	}
}
