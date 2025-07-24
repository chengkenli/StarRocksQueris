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
		util.Loggrs.Warn(uid, "当没有填写飞书应用机器人key时，不支持发送告警信息给个人")
		return
	}

	for _, larkbodys := range body {
		if larkbodys == nil {
			continue
		}
		if larkbodys.Action == 0 {
			continue
		}
		if len(larkbodys.Message) == 0 {
			continue
		}
		logfile := larkbodys.Logfile
		message := larkbodys.Message
		apps := regexp.MustCompile(`App:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "", "*", "").Replace(message))
		user := regexp.MustCompile(`💬User:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "", "*", "").Replace(message))
		connectionId := regexp.MustCompile(`ConnectionId:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "", "*", "").Replace(message))
		submitu := regexp.MustCompile(`💬Submit User:\s*\[\s*(.*?)\s*\]`).FindStringSubmatch(strings.NewReplacer(`\t`, "", "*", "").Replace(message))
		if len(user) < 1 {
			continue
		}

		util.Loggrs.Info(uid, "###------>U :", user, "len:", len(user), "text:", strings.Join(user, ","))
		util.Loggrs.Info(uid, "###------>SU:", submitu, "len:", len(submitu), "text:", strings.Join(submitu, ","))
		userid := user[1]
		app := apps[1]

		// 从缓存中获取指标
		v, ok := meta.OpenIDCache.Get(userid)
		if !ok {
			util.Loggrs.Warn(uid, fmt.Sprintf("没有找到%s的openid.", userid))
			util.Loggrs.Warn(uid, "开始寻找Submit User")
			if len(submitu) < 1 {
				continue
			}
			userid := strings.ToLower(strings.ReplaceAll(submitu[1], " ", ""))
			v, ok = meta.OpenIDCache.Get(userid)
			if !ok {
				util.Loggrs.Warn(uid, fmt.Sprintf("也没有找到关于%s的openid.", userid))
				continue
			}
		}

		value := strings.Split(v.(string), ":")
		if len(value) < 2 {
			continue
		}
		openid := value[0]
		username := value[1]

		util.Loggrs.Info(uid, fmt.Sprintf("通过[openid]分发给个人用户 %v", v))
		// 根据openid发送信息给个人用户
		util.Loggrs.Info(uid, fmt.Sprintf("app:[%s] userid:[%s] username:[%s] openid:[%s]", app, userid, username, openid))
		content := fmt.Sprintf(`{\"elements\":[{\"tag\":\"div\",\"text\":{\"content\":\"您好！慢查询监控系统发现，您有一笔StarRocks查询触发告警 ，以下是详细信息：\\n\\n%s\",\"tag\":\"lark_md\"}},{\"actions\":[{\"tag\":\"button\",\"text\":{\"content\":\"日志\",\"tag\":\"lark_md\"},\"url\":\"%s\",\"type\":\"default\",\"value\":{}}],\"tag\":\"action\"}],\"header\":{\"template\":\"turquoise\",\"title\":{\"content\":\"[A]慢查询告警]\",\"tag\":\"plain_text\"}}}`, cReplace(message), logfile)
		msg := fmt.Sprintf(`{
    "receive_id": "%s",
    "msg_type": "interactive",
    "content": "%s"
}`, openid, content)
		// 飞书应用发送信息
		err := api.SendMessageOpenID(
			&api.OMsg{
				Body: strings.NewReplacer("**", "", "%", "%%").Replace(msg),
			})
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			util.Loggrs.Error(uid, msg)
			continue
		}
		util.Loggrs.Info(uid, fmt.Sprintf("%s %s %s send feishu done.", app, userid, username))
		message = fmt.Sprintf(`集群：%s，账号：%s，责任人：%s，连接ID：%s`, app, userid, username, connectionId[1])
		go Send2Markdown(fmt.Sprintf("给%s发送了一条信息", username), message, logfile, util.Config.GetStringSlice("Schema.lkremind"))
	}

}
