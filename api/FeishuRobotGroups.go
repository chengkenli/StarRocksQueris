/*
 *@author  chengkenli
 *@project StarRocksApp
 *@package apis
 *@file    OpenApi_RobotGroups
 *@date    2024/9/29 14:58
 */

package api

import (
	"StarRocksQueris/util"
	"encoding/json"
)

// robotGroups 获取机器人所在的群列表
func robotGroups() []string {
	token, err := GetTenantAccessToken()
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	//发送POST请求并处理响应
	respones, err := Client.R().
		SetHeaders(map[string]string{
			"Content-Type":  "application/json; charset=utf-8",
			"Authorization": "Bearer " + token,
		}).
		Get("https://open.feishu.cn/open-apis/im/v1/chats?")
	if err != nil {
		return nil
	}
	code, _ := Code(respones.Body())
	if code != 0 {
		util.Loggrs.Warn(string(respones.Body()))
		return nil
	}
	var rg RobotGroup
	err = json.Unmarshal(respones.Body(), &rg)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	var chat []string
	for _, item := range rg.Data.Items {
		chat = append(chat, item.ChatID)
	}
	return chat
}
