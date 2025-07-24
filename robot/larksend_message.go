/*
 *@author  chengkenli
 *@project StarRocksDict
 *@package lark
 *@file    lark_test
 *@date    2025/4/10 15:39
 */

package robot

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/go-resty/resty/v2"
	"sync"
)

func Send2text(title, message, url string, roboots []string) {
	msg := fmt.Sprintf(`
	{
	   "msg_type": "post",
	   "content": {
	       "post": {
	           "zh_cn": {
	               "title": "%s",
	               "content": [
	                   [
	                       {
	                           "tag": "text",
	                           "text": "%s"
	                       },
	                       {
	                           "tag": "a",
	                           "text": " log",
	                           "href": "%s"
	                       }
	                   ]
	               ]
	           }
	       }
	   }
	}`, title, message, url)

	var wg sync.WaitGroup
	for _, roboot := range roboots {
		wg.Add(1)
		go func(roboot string) {
			defer wg.Done()
			r := restys(fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", roboot), msg)
			util.Loggrs.Info(string(r))
		}(roboot)
	}
	wg.Wait()
}

func restys(uri, body string) []byte {
	//发送POST请求并处理响应
	respones, err := resty.New().SetProxy(util.ConnectNorm.SlowQueryProxyFeishu).R().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetBody(body).
		Post(uri)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(string(respones.Body()))
	return respones.Body()
}
