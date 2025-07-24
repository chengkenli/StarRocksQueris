/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_def
 *@date    2024/11/6 15:41
 */

package pipe

import (
	"StarRocksQueris/pool"
	"time"
)

// 初始化缓存实例
var (
	Concurrencylimit *pool.CacheWrapper
	LarkConnectionId *pool.CacheWrapper
	MailConnectionId *pool.CacheWrapper
)

func init() {
	Concurrencylimit = pool.InstantiationCache("Concurrencylimit", 5*time.Minute, 10*time.Minute)
	LarkConnectionId = pool.InstantiationCache("LarkConnectionId", 10*time.Minute, 10*2*time.Minute)
	MailConnectionId = pool.InstantiationCache("MailConnectionId", 10*time.Minute, 10*2*time.Minute)
}
