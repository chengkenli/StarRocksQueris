/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_time
 *@date    2025/1/10 14:51
 */

package short

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

// 中间转换时间，主要应用与区间保障
func (j *Job) centerTransTime(c *util.ReData, status int) string {
	sign := strings.Split(c.Ctime, ",")
	if len(sign) < 3 {
		return c.Ctime
	}
	ctime := sign[0]
	cnmin := sign[1]
	fbmin := sign[2]

	//判断当前时间是否在保障范围中
	if _, ok := isTime(ctime); !ok {
		if _, b := lastTime.Get(c.App + c.Username); b {
			loggrs.Info(uid, fmt.Sprintf("Delete K>[%s]保障已经不在范围内，清除[%s]已有的缓存数据", c.App+c.Username, c.App+c.Username))
			lastTime.Delete(c.App + c.Username)
		}
		return c.Ctime
	}

	v, ok := lastTime.Get(c.App + c.Username)
	if v != nil {
		loggrs.Info(uid, "LastTime>", v.(string))
	}
	if ok {
		if _, t := isTime(v.(string)); t {
			loggrs.Info(uid, fmt.Sprintf("Cache   >Return k:%s|v:%s", c.App+c.Username, v.(string)))
			lastTime.Set(c.App+c.Username, v.(string), cache.DefaultExpiration)
			return v.(string)
		} else if time.Now().Format("15:04") == strings.Split(v.(string), "-")[1] {

			if status == 0 {
				lastSt, _ := time.Parse("15:04", strings.Split(v.(string), "-")[0])
				lastEt, _ := time.Parse("15:04", strings.Split(v.(string), "-")[1])
				value := lastSt.Add(time.Hour).Format("15:04") + "-" + lastEt.Add(time.Hour).Format("15:04")
				msg := fmt.Sprintf("State   >Return k:%s|v:%s", c.App+c.Username, value)
				loggrs.Info(uid, msg)
				loggrs.Info(uid, "Set Cahe>缓存载入新的时间区间 ", value)
				lastTime.Set(c.App+c.Username, value, cache.DefaultExpiration)
				return value
			}
			if time.Now().Second() == 0 {
				//time.Sleep(time.Second)
				j.Donec <- c.App + "^close#" + fmt.Sprintf("Close   >(%v)[%s] (%d)(%d)(%d) - %s ", v.(interface{}), ctime, -1, -1, -1, c.Username)
			}
			msg := fmt.Sprintf("Equal   >Return k:%s|v:%s", c.App+c.Username, v.(string))
			loggrs.Info(uid, msg)
			return v.(string)
		} else {
			msg := fmt.Sprintf("Split   >Return k:%s|v:%s", c.App+c.Username, v.(string))
			loggrs.Info(uid, msg)
			return v.(string)
		}
	}

	minute, _ := strconv.Atoi(cnmin)
	loc, _ := time.LoadLocation("Local")

	var hour int
	if time.Now().Minute() == 0 {
		hour = time.Now().Hour() + 1
	} else {
		hour = time.Now().Hour()
	}

	center := time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), hour, minute, 0, 0, loc)

	minute_center, _ := strconv.Atoi(fbmin)

	loggrs.Info(uid, "当前监控指标：", center.Format("2006-01-02 15:04:05"))
	if time.Now().Sub(center).Seconds() >= 1 {
		loggrs.Info(uid, "由于当前时间已经大于监控指标，进行+1hour再精准")
		center = center.Add(time.Hour)
	}
	// 减去10分钟
	before := center.Add(time.Duration(-minute_center) * time.Minute).Format("15:04")
	// 增加10分钟
	after := center.Add(time.Duration(+minute_center) * time.Minute).Format("15:04")

	//时间间距
	center_time := before + "-" + after

	//把时间载入缓存中
	loggrs.Info(uid, fmt.Sprintf("Cache   >Set k:%s|v:%s", c.App+c.Username, center_time))
	lastTime.Set(c.App+c.Username, center_time, cache.DefaultExpiration)

	return center_time
}
