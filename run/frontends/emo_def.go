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
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
	"strings"
	"sync"
	"time"
)

var (
	sessionConcurrencylimitCache = cache.New(5*time.Minute, 10*time.Minute)
	FsCache                      = cache.New(10*time.Minute, 10*2*time.Minute)
	EmCache                      = cache.New(10*time.Minute, 10*2*time.Minute)
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

	warn := tools.GetHour(int(util.ConnectNorm.SlowQueryTime))
	kill := tools.GetHour(util.ConnectNorm.SlowQueryKtime)

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
func protect(user string, mutex *sync.Mutex) bool {
	mutex.Lock()
	// 判断当前用户是否为百名单，白名单不进行处理
	if len(util.ConnectNorm.SlowQueryFocususer) != 0 {
		if tools.StringInSlice(user, strings.Split(util.ConnectNorm.SlowQueryFocususer, ",")) {
			return true
		}
	}
	mutex.Unlock()
	return false
}

//
//// 飞书主体去重函数
//func deduplicateLarkbodies(larkbodies []*util.Larkbodys) []*util.Larkbodys {
//	unique := make([]*util.Larkbodys, 0)
//	seen := make(map[string]bool)
//
//	for _, l := range larkbodies {
//		// 创建一个唯一的key来表示Larkbody的内容
//		key := fmt.Sprintf("%v-%v", l.Field1, l.Field2)
//		if _, exists := seen[key]; !exists {
//			seen[key] = true
//			unique = append(unique, l)
//		}
//	}
//
//	return unique
//}

// exists checks if a *Larkbody is in the []*Larkbody slice.
func existsLarkbodys(slice []*util.Larkbodys, item *util.Larkbodys) bool {
	for _, v := range slice {
		if v == item { // 这里是比较指针是否相同
			return true
		}
		// 如果结构体的字段可以比较，并且你想比较结构体的内容是否相同，可以使用以下方式：
		// if v.ID == item.ID && v.Name == item.Name {
		//     return true
		// }
	}
	return false
}

// exists checks if a *SchemaData is in the []*SchemaData slice.
func existsSchemaData(slice []*util.SchemaData, item *util.SchemaData) bool {
	for _, v := range slice {
		if v == item { // 这里是比较指针是否相同
			return true
		}
		// 如果结构体的字段可以比较，并且你想比较结构体的内容是否相同，可以使用以下方式：
		// if v.ID == item.ID && v.Name == item.Name {
		//     return true
		// }
	}
	return false
}
