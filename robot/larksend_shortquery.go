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
	"strings"
)

// SendFsCartShortQuery global
func SendFsCartShortQuery(larkbodys *util.Larkbodys) {
	var msgs []string
	if larkbodys == nil {
		return
	}
	if len(larkbodys.Message) == 0 {
		return
	}
	msgs = append(msgs, fmt.Sprintf(`
{
                "tag": "div",
                "text": {
                    "content": "%s",
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
                    },
					{
                        "tag": "button",
                        "text": {
                            "content": "说明",
                            "tag": "lark_md"
                        },
                        "url": "",
                        "type": "default",
                        "value": {
                            
                        }
                    }
                ],
                "tag": "action"
            }`, larkbodys.Message, larkbodys.Logfile))

	var title string
	switch larkbodys.Action {
	case -1:
		title = "短查询保障(SLEEP)"
	case 0:
		title = "短查询保障(CLOSE)"
	case 1:
		title = "短查询保障(OPEN)"
	case 2:
		title = "短查询保障(ON GOING)"
	default:
		title = "短查询保障(NIL)"
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

	var robots []string
	for _, m := range tools.UniqueMaps(util.ConnectRobot) {
		if m["type"] != nil {
			if m["key"] == nil {
				continue
			}
			if m["robot"] == nil {
				continue
			}
			//app := m["key"].(string)
			mold := m["type"].(string)

			if mold == "global" {
				robots = append(robots, m["robot"].(string))
				break
			}
		}
	}

	if robots == nil {
		return
	}
	for _, robot := range robots {
		r := SendFsPost("POST", fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", robot), strings.NewReader(msg))
		if strings.Contains(string(r), "Bad Request") || strings.Contains(string(r), "err") {
			util.Loggrs.Error(uid, msg)
		}
		util.Loggrs.Info(uid, string(r))
	}
}
