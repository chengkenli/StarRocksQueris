/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    SendFsCartApp
 *@date    2024/9/13 16:42
 */

package robot

import (
	"StarRocksQueris/api"
	"StarRocksQueris/meta"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"regexp"
	"strings"
)

// SendFsCartOpenID2User 通过[openid]分发给个人用户
func SendFsCartOpenID2User(body []*util.Larkbodys) {

	if !tools.AuthLarkApp() {
		util.Loggrs.Warn("当没有填写飞书应用机器人key时，不支持发送告警信息给个人")
		return
	}

	for _, larkbodys := range body {
		if len(larkbodys.Message) == 0 {
			continue
		}
		logfile := larkbodys.Logfile
		message := larkbodys.Message
		apps := regexp.MustCompile(`App:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "").Replace(message))
		user := regexp.MustCompile(`User:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "").Replace(message))
		if len(user) < 1 {
			continue
		}

		userid := user[1]
		app := apps[1]

		// 从缓存中获取指标
		v, ok := meta.OpenIDCache.Get(userid)
		if !ok {
			util.Loggrs.Warn(fmt.Sprintf("没有找到%s的openid.", userid))
			continue
		}
		value := strings.Split(v.(string), ":")
		if len(value) < 2 {
			continue
		}
		openid := value[0]
		username := value[1]

		util.Loggrs.Info(fmt.Sprintf("通过[openid]分发给个人用户"))

		// 根据openid发送信息给个人用户
		util.Loggrs.Info(fmt.Sprintf("app:[%s] userid:[%s] username:[%s] openid:[%s]", app, userid, username, openid))
		content := fmt.Sprintf(`{\"elements\":[{\"tag\":\"div\",\"text\":{\"content\":\"您好！慢查询监控系统发现，您有一笔StarRocks查询触发告警 ，以下是详细信息：\\n\\n%s\",\"tag\":\"lark_md\"}},{\"actions\":[{\"tag\":\"button\",\"text\":{\"content\":\"日志\",\"tag\":\"lark_md\"},\"url\":\"%s\",\"type\":\"default\",\"value\":{}}],\"tag\":\"action\"}],\"header\":{\"template\":\"turquoise\",\"title\":{\"content\":\"[A]慢查询告警]\",\"tag\":\"plain_text\"}}}`, cReplace(message), logfile)
		msg := fmt.Sprintf(`{
    "receive_id": "%s",
    "msg_type": "interactive",
    "content": "%s"
}`, openid, content)
		// 飞书应用发送信息
		err := api.SendMessageOpenID(
			&api.OMsg{
				Body: strings.NewReplacer("**", "").Replace(msg),
			})
		if err != nil {
			util.Loggrs.Error(err.Error())
			util.Loggrs.Error(msg)
			continue
		}
		util.Loggrs.Info(fmt.Sprintf("%s %s %s send feishu done.", app, userid, username))
		SendFsText("Ok", fmt.Sprintf("app:[%s] userid:[%s] username:[%s] send feishu done.", app, userid, username), "", []string{"2717b942-0a19-4248-a495-f290380ae7b4"})
	}

}
