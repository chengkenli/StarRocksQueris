/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package license
 *@file    licenseSelect
 *@date    2024/8/19 13:55
 */

package license

import (
	"StarRocksQueris/robot"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/robfig/cron/v3"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Sessionlicense() {
	crontab := cron.New()
	// 添加定时任务, * * * * * 是 crontab,表示每分钟执行一次
	_, err := crontab.AddFunc("0 8 * * *", func() {

		if len(tools.UniqueMaps(util.ConnectBody)) == 0 {
			return
		}

		// job stsrt
		var wg sync.WaitGroup
		var messages []string
		for _, body := range tools.UniqueMaps(util.ConnectBody) {
			wg.Add(1)
			go func(body map[string]interface{}) {
				defer wg.Done()

				if _, ok := body["address"]; ok {
					msgs, day, err := applicense(body)
					if err != nil {
						util.Loggrs.Error(err.Error())
						return
					}

					if day <= body["expire"].(int) {
						msg := fmt.Sprintf(`\n\n集群:[%s]，license即将在[%d]天后过期\n%v`, body["app"].(string), day, msgs)
						messages = append(messages, msg)
					}
				}
			}(body)
		}
		wg.Wait()

		for _, m := range tools.UniqueMaps(util.ConnectRobot) {
			if m["type"] == "global" {
				if messages != nil {
					if v, ok := m["robot"]; ok {
						robot.SendFsText("starrocks license过期提醒", strings.Join(messages, "\n\n"), "", strings.Split(v.(string), ","))
					}
				}
			}
		}

		// job end
	})
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	// 启动定时器
	crontab.Start()
	// 定时任务是另起协程执行的,这里使用 select 简答阻塞.实际开发中需要
	// 根据实际情况进行控制
	select {}
}

// applicense 检查集群license过期信息
func applicense(body map[string]interface{}) ([]string, int, error) {
	var msg []string
	type license struct {
		Code int `json:"code"`
		List []struct {
			Cores    int   `json:"cores"`
			ExpireAt int64 `json:"expire_at"`
			Hosts    int   `json:"hosts"`
		} `json:"list"`
		Total int `json:"total"`
	}
	type key struct {
		Code int `json:"code"`
		Data struct {
			Cores int    `json:"cores"`
			D     string `json:"d"`
		} `json:"data"`
	}

	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应
	_, err := Client.R().SetBody(map[string]string{
		"name":     body["user"].(string),
		"password": body["password"].(string),
	}).Post(body["address"].(string) + "/api/user/login")
	if err != nil {
		return nil, -1, err
	}
	/*------------------------------------------------*/
	respones2, err := Client.R().Get(body["address"].(string) + "/api/license/list")
	if err != nil {
		return nil, -1, err
	}
	/*------------------------------------------------*/
	respones3, err := Client.R().Get(body["address"].(string) + "/api/license/collect-hosts-info")
	if err != nil {
		return nil, -1, err
	}
	/*------------------------------------------------*/
	var k key
	err = json.Unmarshal(respones3.Body(), &k)
	if err != nil {
		return nil, -1, err
	}

	var l license
	err = json.Unmarshal(respones2.Body(), &l)
	if err != nil {
		return nil, -1, err
	}

	var m []int
	for _, s := range l.List {
		day := getday(fmt.Sprintf("%d", time.Now().UnixNano()/1e6), fmt.Sprintf("%d", s.ExpireAt))
		m = append(m, day)
		msg = append(msg, fmt.Sprintf("主机数量：%d，核心数量：%d，过期时间：%s，license：%s", s.Hosts, s.Cores, unixToTime(strconv.FormatInt(s.ExpireAt, 10)).Format("2006-01-02 15:04:05"), k.Data.D))
	}
	return msg, findMax(m), nil
}

func getday(date1Str, date2Str string) int {
	// 将字符串转化为Time格式
	date1, err := time.ParseInLocation("2006-01-02", unixToTime(date1Str).Format("2006-01-02"), time.Local)
	if err != nil {
		return 0
	}
	// 将字符串转化为Time格式
	date2, err := time.ParseInLocation("2006-01-02", unixToTime(date2Str).Format("2006-01-02"), time.Local)
	if err != nil {
		return 0
	}
	//计算相差天数
	return int(date2.Sub(date1).Hours() / 24)
}

func unixToTime(e string) (datatime time.Time) {
	data, _ := strconv.ParseInt(e, 10, 64)
	datatime = time.Unix(data/1000, 0)
	return
}

// findMax 返回切片中的最大整数
func findMax(slice []int) int {
	if len(slice) == 0 {
		return 0 // 如果切片为空，返回0或根据需要处理错误
	}
	max := slice[0] // 假设第一个元素是最大的
	for _, value := range slice {
		if value > max {
			max = value // 更新最大值
		}
	}
	return max
}
