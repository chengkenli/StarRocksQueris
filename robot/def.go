/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    def
 *@date    2024/9/13 16:40
 */

package robot

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

type WarnEmail struct {
	App          string
	Log          string
	Schema       []string
	TfIdfs       []string
	BucketStatus bool
	Avgs         []string
	SCache       *cache.Cache
	Item         *util.Process2
	Fullscan     bool
	Queris       *util.InQue
}

type WarnQuerisEmail struct {
	Avgs   []string
	Queris *util.InQue
}

func bug(msg string) string {
	compile, _ := regexp.Compile(`\s+`)
	m := compile.ReplaceAllString(msg, " ")

	return strings.NewReplacer(
		"\n", `\n`,
	).Replace(m)
}

func textDebug(msg string) string {
	compile, _ := regexp.Compile(`\s+`)
	m := compile.ReplaceAllString(msg, " ")

	return strings.NewReplacer(
		"\n", `\n`,
		"'", "",
	).Replace(m)
}

func cReplace(msg string) string {
	compile, _ := regexp.Compile(`\s+`)
	m := compile.ReplaceAllString(msg, " ")

	return strings.NewReplacer(
		`\n`, `\\n`,
		`\t`, `\\t`,
		"'", "",
	).Replace(m)
}

func SendFsPost(method, u string, body io.Reader) []byte {
	request, err := http.NewRequest(method, u, body)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	request.Header.Set("Content-Type", "application/json;charset=utf-8")

	var client *http.Client
	if len(util.ConnectNorm.SlowQueryProxyFeishu) != 0 {
		proxy, _ := url.Parse(util.ConnectNorm.SlowQueryProxyFeishu)
		client = &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxy),
			},
		}
	} else {
		client = &http.Client{
			Timeout:   time.Second * 30,
			Transport: &http.Transport{},
		}
	}
	respone, err := client.Do(request)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	defer respone.Body.Close()
	b, err := ioutil.ReadAll(respone.Body)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	return b
}

func SendFsText(title, message, url string, roboots []string) {
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
	}`, title, bug(message), url)

	var wg sync.WaitGroup
	for _, roboot := range roboots {
		wg.Add(1)
		go func(roboot string) {
			defer wg.Done()
			r := SendFsPost("POST", fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", roboot), strings.NewReader(msg))
			util.Loggrs.Info(string(r))
		}(roboot)
	}
	wg.Wait()
}
