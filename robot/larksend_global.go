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
)

// SendFsCartGlobal global
func SendFsCartGlobal(body []*util.Larkbodys) {
	if body == nil {
		return
	}
	var msgInfo, msgWarn []string
	for i, larkbodys := range body {
		if larkbodys == nil {
			continue
		}
		if len(larkbodys.Message) == 0 {
			continue
		}

		msg := fmt.Sprintf(`
{
                "tag": "div",
                "text": {
                    "content": "#%d\n%s",
                    "tag": "lark_md"
                }
            },
            {
                "actions": [
                    %s
                ],
                "tag": "action"
            }`, i, larkbodys.Message, larkbodys.Button)

		if larkbodys.Action == 0 || larkbodys.Action == 2 {
			msgInfo = append(msgInfo, msg)
		} else {
			msgWarn = append(msgWarn, msg)
		}
	}
	// 进入debug模式
	if util.P.Check {
		if len(msgInfo) != 0 {
			SendFsCart2Debug(msgInfo)
		}
		if len(msgWarn) != 0 {
			SendFsCart2Debug(msgWarn)
		}
		return
	}
	// Info & Warn
	if len(msgInfo) != 0 {
		SendFsCart2Info(msgInfo)
	}
	if len(msgWarn) != 0 {
		SendFsCart2Warn(msgWarn)
	}
}
