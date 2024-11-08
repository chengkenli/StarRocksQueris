/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package tools
 *@file    object
 *@date    2024/8/7 14:57
 */

package tools

import (
	"StarRocksQueris/util"
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type SrAvgs struct {
	Host string
	Port int
	User string
	Pass string
}

// GetHour 秒格式化
func GetHour(second int) string {
	hours := second / 3600
	minutes := (second % 3600) / 60
	secs := second % 60

	if hours >= 1 {
		return fmt.Sprintf("%02dh:%02dmin:%02ds", hours, minutes, secs)
	}
	if minutes >= 1 {
		return fmt.Sprintf("%02dmin:%02ds", minutes, secs)
	}
	return fmt.Sprintf("%02ds", secs)
}

// WriteFile 文件落地
func WriteFile(fname, msg string) {
	fileHandle, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	defer fileHandle.Close()
	// NewWriter 默认缓冲区大小是 4096
	// 需要使用自定义缓冲区的writer 使用 NewWriterSize()方法
	buf := bufio.NewWriterSize(fileHandle, len(msg))

	buf.WriteString(msg)

	err = buf.Flush()
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
}

// RemoveDuplicateStrings /*数组去重*/
func RemoveDuplicateStrings(strs []string) []string {
	result := []string{}
	tempMap := map[string]byte{} // 存放不重复字符串
	for _, e := range strs {
		l := len(tempMap)
		tempMap[e] = 0
		if len(tempMap) != l { // 加入map后，map长度变化，则元素不重复
			result = append(result, e)
		}
	}
	return result
}

// StringInSlice 检查数组中是否存在某个元素
func StringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}

// 寻找元素位置
func FindKeyRank(slice []map[string]int64, searchKey string) (int, bool) {
	rank := 0 // 排行计数器
	// 遍历切片中的每个map
	for _, m := range slice {
		// 检查map中是否存在指定的键
		if _, ok := m[searchKey]; ok {
			// 如果找到了匹配的键，返回排行和true
			return rank, true // 排名从1开始计算
		}
		// 增加排行计数器，即使当前map中没有找到键
		rank += len(m)
	}
	// 如果没有找到，返回0和false
	return 0, false
}

// SumMapValues 定义一个辅助函数，用于计算map中所有int64值的总和
func SumMapValues(m map[string]int64) int64 {
	sum := int64(0)
	for _, v := range m {
		sum += v
	}
	return sum
}

// Post 一个POST方法
func Post(method, u string, body io.Reader) []byte {
	request, err := http.NewRequest(method, u, body)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	request.Header.Set("Content-Type", "application/json;charset=utf-8")
	client := &http.Client{
		Timeout:   time.Second * 30,
		Transport: &http.Transport{},
	}
	respone, err := client.Do(request)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	defer respone.Body.Close()
	b, err := ioutil.ReadAll(respone.Body)
	if err != nil {
		util.Loggrs.Error(err)
		return nil
	}
	return b
}

func HostApp(app string) string {
	if len(util.ConnectBody) == 0 {
		return ""
	}
	for _, m := range util.ConnectBody {
		if m["app"].(string) == app {
			if m["feip"] != nil {
				return m["feip"].(string)
			}
		}
	}
	return ""
}

func RangerMap(key string, slice []map[string]int) int {
	key = strings.NewReplacer(" ", "").Replace(key)
	for _, m := range slice {
		// 检查map中是否存在给定的key
		if value, ok := m[key]; ok {
			return value // 找到key，返回其对应的value和true
		}
	}
	return -1
}

func Int64(str string) int64 {
	parseInt, err := strconv.ParseFloat(strings.NewReplacer(" ", "").Replace(strings.Split(str, " ")[0]), 64)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return int64(parseInt)
	}
	return int64(parseInt)
}

// MaxFloat64 返回切片中的最大值
func MaxFloat64(numbers []float64) float64 {
	if len(numbers) == 0 {
		// 如果切片为空，返回一个错误或合适的值
		return 0
	}
	// 假设第一个元素是最大值
	max := numbers[0]
	// 遍历切片中的每个元素
	for _, v := range numbers {
		// 如果当前元素比已知的最大值大，则更新最大值
		if v > max {
			max = v
		}
	}
	// 返回最大值
	return max
}

// AuthRegis 用于判断审计表的信息是否有填写
func AuthRegis() bool {
	if util.ConnectNorm["slow_query_data_registration_username"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_data_registration_password"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_data_registration_table"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_data_registration_host"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_data_registration_port"] == nil {
		return false
	}
	return true
}

// AuthLarkApp 验证飞书应用机器人，是否已经填写了key
func AuthLarkApp() bool {
	if util.ConnectNorm["slow_query_lark_app"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_lark_appid"] == nil {
		return false
	}
	if util.ConnectNorm["slow_query_lark_appsecret"] == nil {
		return false
	}
	return true
}
