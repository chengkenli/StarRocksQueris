/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package app
 *@file    short
 *@date    2024/11/21 13:58
 */

package short

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 进度条
func shortBar(status int, initial, ctime string) (string, float64) {
	split := strings.Split(ctime, "-")

	//获取区间秒数
	sTime, err := time.Parse("15:04", split[0])
	if err != nil {
		loggrs.Error(uid, "Error parsing start time:", err)
	}
	eTime, err := time.Parse("15:04", split[1])
	if err != nil {
		loggrs.Error(uid, "Error parsing end time:", err)
	}
	//if strings.Contains(initial, ",") && status == 0 {
	//	sTime = sTime.Add(-time.Hour)
	//	eTime = eTime.Add(-time.Hour)
	//}
	loggrs.Info(uid, fmt.Sprintf("%s-%s", sTime.Format("15:04"), eTime.Format("15:04")))
	// 计算时间差
	totalTime := eTime.Sub(sTime)
	//end
	esH, _ := strconv.Atoi(strings.Split(eTime.Format("15:04"), ":")[0])
	esM, _ := strconv.Atoi(strings.Split(eTime.Format("15:04"), ":")[1])
	// 获取当前时间
	now := time.Now()
	// 设置结束时间
	endTime := time.Date(now.Year(), now.Month(), now.Day(), esH, esM, 0, 0, now.Location())
	// 如果当前时间已经超过了18:00，则设置结束时间为下一天的18:00
	if now.After(endTime) {
		endTime = endTime.AddDate(0, 0, 1)
	}
	// 计算总时间（一天的秒数）
	//totalTime := 24 * 60 * 60 * time.Second
	// 计算剩余时间（秒）
	remainingTime := endTime.Sub(now)
	// 计算两个时间差
	loggrs.Info(uid, "计算剩余时间（秒）: ", remainingTime.Seconds())
	loggrs.Info(uid, "计算总共时间（秒）: ", totalTime.Seconds())

	loggrs.Info(uid, "当前时间: ", now.Format("15:04"))
	loggrs.Info(uid, "结束时间: ", eTime.Format("15:04"))

	var percentage float64
	if int(endTime.Sub(now).Seconds()) > 80000 {
		percentage = 100
	} else {
		if now.Format("15:04") == eTime.Format("15:04") {
			loggrs.Info(uid, "当前时间与结束时间对等，百分比赋予100%")
			percentage = 100
		} else {
			loggrs.Info(uid, "当前时间与结束时间不对等，计算百分比")
			percentage = (1 - (remainingTime.Seconds() / totalTime.Seconds())) * 100
		}
	}
	loggrs.Info(uid, "当前进度百分比: ", percentage)
	// 创建进度条
	barLength := 20 // 可以根据需要调整进度条长度
	bar := make([]rune, barLength)
	for i := 0; i < int(percentage/100*float64(barLength)); i++ {
		bar[i] = '■'
	}
	for i := int(percentage / 100 * float64(barLength)); i < barLength; i++ {
		bar[i] = '□'
	}
	//// 打印进度条
	//fmt.Printf("Progress: [%s] %.2f%%\n", string(bar), percentage)
	return string(bar), percentage
}
