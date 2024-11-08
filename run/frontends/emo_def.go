/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_def
 *@date    2024/11/6 15:41
 */

package fronends

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"strings"
)

type handle struct {
	Connect *gorm.DB
	App     string
	Fe      string
	Queries *util.Queris
	Item    *util.Process2
}
type Workers struct {
	allTasks     chan string
	runningTasks chan string
	pendingTasks chan string
	lark         chan *util.Larkbodys
	data         chan *util.SchemaData
}

// Singnel 每种拦截行为的标志
func Singnel(action int) string {

	warn := tools.GetHour(int(util.ConnectNorm["slow_query_time"].(int32)))
	kill := tools.GetHour(int(util.ConnectNorm["slow_query_ktime"].(int32)))

	switch action {
	case 0:
		return "⓿.状态异常停留清退"
	case 1:
		return "①.异常违规参数查杀"
	case 2:
		return fmt.Sprintf("②.%s慢查询提醒", warn)
	case 3:
		return fmt.Sprintf("③.%s慢查询查杀", kill)
	case 4:
		return "④.全表扫描亿级查杀"
	case 5:
		return "⑤.TB级扫描字节查杀"
	case 6:
		return "⑥.百亿扫描行数查杀"
	case 7:
		return "⑦.CATALOG违规查杀"
	case 8:
		return "⑧.GB级消耗内存查杀"
	default:
	}
	return ""
}

// 判断用户是否属于白名单
func protect(user string) bool {
	// 判断当前用户是否为百名单，白名单不进行处理
	if util.ConnectNorm["slow_query_focususer"] != nil {
		if tools.StringInSlice(user, strings.Split(util.ConnectNorm["slow_query_focususer"].(string), ",")) {
			return true
		}
	}
	return false
}
