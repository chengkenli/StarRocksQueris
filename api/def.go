/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package api
 *@file    def
 *@date    2024/9/12 16:28
 */

package api

type OneStop2ID struct {
	Count   int `json:"count"`
	Message []struct {
		ClusterSortName string `json:"ClusterSortName"`
		Account         string `json:"Account"`
		UserID          string `json:"UserId"`
		DisplayName     string `json:"DisplayName"`
		Dn              string `json:"Dn"`
		Email           string `json:"Email"`
		Leader          string `json:"Leader"`
		DirectReports   string `json:"DirectReports"`
		IsEnable        bool   `json:"IsEnable"`
	} `json:"message"`
	StatusCode int `json:"statusCode"`
}

type Chats struct {
	Openid   string `json:"openid"`
	Chatid   string `json:"chatid"`
	Username string `json:"username"`
	Userid   string `json:"userid"`
}
type OMsg struct {
	Body string
}

type MeMbers struct {
	User   string
	ChatID string
	Token  string
}

type TenantToken struct {
	Code   int    `json:"code"`
	Expire int    `json:"expire"`
	Msg    string `json:"msg"`
	Token  string `json:"tenant_access_token"`
}
type AppToken struct {
	AppAccessToken    string `json:"app_access_token"`
	Code              int    `json:"code"`
	Expire            int    `json:"expire"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
}

type UserBody struct {
	Code int `json:"code"`
	Data struct {
		HasMore bool `json:"has_more"`
		Items   []struct {
			Avatar struct {
				Avatar240    string `json:"avatar_240"`
				Avatar640    string `json:"avatar_640"`
				Avatar72     string `json:"avatar_72"`
				AvatarOrigin string `json:"avatar_origin"`
			} `json:"avatar"`
			Description   string `json:"description"`
			EnName        string `json:"en_name"`
			MobileVisible bool   `json:"mobile_visible"`
			Name          string `json:"name"`
			OpenID        string `json:"open_id"`
			UnionID       string `json:"union_id"`
			Nickname      string `json:"nickname,omitempty"`
		} `json:"items"`
	} `json:"data"`
	Msg string `json:"msg"`
}

type GroupMembers struct {
	Code int `json:"code"`
	Data struct {
		HasMore bool `json:"has_more"`
		Items   []struct {
			MemberID     string `json:"member_id"`
			MemberIDType string `json:"member_id_type"`
			Name         string `json:"name"`
			TenantKey    string `json:"tenant_key"`
		} `json:"items"`
		MemberTotal int    `json:"member_total"`
		PageToken   string `json:"page_token"`
	} `json:"data"`
	Msg string `json:"msg"`
}

type RobotGroup struct {
	Code int `json:"code"`
	Data struct {
		HasMore bool `json:"has_more"`
		Items   []struct {
			Avatar      string `json:"avatar"`
			ChatID      string `json:"chat_id"`
			ChatStatus  string `json:"chat_status"`
			Description string `json:"description"`
			External    bool   `json:"external"`
			Name        string `json:"name"`
			OwnerID     string `json:"owner_id"`
			OwnerIDType string `json:"owner_id_type"`
			TenantKey   string `json:"tenant_key"`
		} `json:"items"`
		PageToken string `json:"page_token"`
	} `json:"data"`
	Msg string `json:"msg"`
}
