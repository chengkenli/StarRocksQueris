/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package api
 *@file    FeishuApp
 *@date    2024/9/12 16:28
 */

package api

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/patrickmn/go-cache"
	"time"
)

var Client *resty.Client
var Cache *cache.Cache

var App, Appid, AppSecret string

func init() {
	if util.ConnectNorm["slow_query_lark_app"] != nil {
		App = util.ConnectNorm["slow_query_lark_app"].(string)
	}
	if util.ConnectNorm["slow_query_lark_appid"] != nil {
		Appid = util.ConnectNorm["slow_query_lark_appid"].(string)
	}
	if util.ConnectNorm["slow_query_lark_appsecret"] != nil {
		AppSecret = util.ConnectNorm["slow_query_lark_appsecret"].(string)
	}
	//创建Resty客户端
	if util.ConnectNorm["slow_query_proxy_feishu"] != nil {
		Client = resty.New().SetProxy(util.ConnectNorm["slow_query_proxy_feishu"].(string))
	} else {
		Client = resty.New()
	}
	Cache = cache.New(20*time.Minute, 20*time.Minute)
}

// GetGroupMembers 根据英文名从【StarRocks】总群中遍历拿到openid
// @username 用户名
// @chatid 群ID
// @token tenant_access_token或者user_access_token
func GetGroupMembers(u *MeMbers) (string, error) {
	util.Loggrs.Info(fmt.Sprintf("%v", u))
	//发送POST请求并处理响应
	uri := fmt.Sprintf("https://open.feishu.cn/open-apis/im/v1/chats/%s/members?member_id_type=open_id", u.ChatID)
	util.Loggrs.Info(uri)
	respones, err := Client.R().
		SetHeader("Content-Type", "application/json; charset=utf-8").
		SetHeader("Authorization", "Bearer "+u.Token).
		Get(uri)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return "", err
	}
	code, _ := Code(respones.Body())
	if code != 0 {
		return "", errors.New(string(respones.Body()))
	}

	var g GroupMembers
	err = json.Unmarshal(respones.Body(), &g)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return "", err
	}
	for _, item := range g.Data.Items {
		if item.Name == u.User {
			return item.MemberID, nil
		}
	}

	for i := 0; i < g.Data.MemberTotal/100; i++ {
		respones, err := Client.R().
			SetHeader("Content-Type", "application/json; charset=utf-8").
			SetHeader("Authorization", "Bearer "+u.Token).
			Get(fmt.Sprintf("https://open.feishu.cn/open-apis/im/v1/chats/%s/members?member_id_type=open_id&page_size=100&page_token=%s", u.ChatID, g.Data.PageToken))
		if err != nil {
			util.Loggrs.Error(err.Error())
			continue
		}
		code, _ := Code(respones.Body())
		if code != 0 {
			util.Loggrs.Warn(string(respones.Body()))
			continue
		}

		var g GroupMembers
		err = json.Unmarshal(respones.Body(), &g)
		if err != nil {
			util.Loggrs.Error(err.Error())
			continue
		}
		for _, item := range g.Data.Items {
			if item.Name == u.User {
				return item.MemberID, nil
			}
		}
	}
	return "", nil
}

// SendMessageOpenID 根据openid发送信息给用户
// @openid openid
func SendMessageOpenID(o *OMsg) error {
	token, err := GetTenantAccessToken()
	if err != nil {
		return err
	}
	//发送POST请求并处理响应
	respones, err := Client.R().
		SetHeader("Authorization", "Bearer "+token).
		SetBody(o.Body).
		Post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id")
	if err != nil {
		return err
	}
	code, _ := Code(respones.Body())
	if code != 0 {
		util.Loggrs.Warn(string(respones.Body()))
		return errors.New(string(respones.Body()))
	}

	util.Loggrs.Info(string(respones.Body()))
	util.Loggrs.Info("send done.")
	return nil
}

// GetTenantAccessToken 获取访问凭证 tenant_access_token
func GetTenantAccessToken() (string, error) {
	if !tools.AuthLarkApp() {
		util.Loggrs.Warn("配置表中飞书应用机器人token没填写！")
		return "", nil
	}
	v, Ok := Cache.Get(App)
	if Ok {
		return v.(string), nil
	}
	//发送POST请求并处理响应
	respones, err := Client.R().
		SetHeader("Content-Type", "application/json; charset=utf-8").
		SetBody(map[string]string{
			"app_id":     Appid,
			"app_secret": AppSecret,
		}).Post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
	if err != nil {
		return "", err
	}
	code, _ := Code(respones.Body())
	if code != 0 {
		util.Loggrs.Warn(string(respones.Body()))
		return "", errors.New(string(respones.Body()))
	}

	var token TenantToken
	err = json.Unmarshal(respones.Body(), &token)
	if err != nil {
		return "", err
	}
	Cache.Set(App, token.Token, cache.DefaultExpiration)
	return token.Token, nil
}

func Code(body []byte) (int64, error) {
	// 创建一个map来存储解析后的数据
	data := make(map[string]interface{})
	// 解析JSON字符串到map中
	err := json.Unmarshal(body, &data)
	if err != nil {
		return -1, err
	}
	// 获取code字段的值
	code, ok := data["code"].(float64) // JSON中的数字默认解析为float64
	if !ok {
		return -1, errors.New("code failed")
	}
	return int64(code), nil
}
