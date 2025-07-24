/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_commit
 *@date    2024/11/22 9:40
 */

package short

import (
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
)

var shortInfo ReDatas

func (j *Job) Conmit(db *gorm.DB, cannel chan struct{}) {
	var mc ReDatas
	r := db.Raw(fmt.Sprintf("select * from %s where status > 0", shortdb)).Scan(&mc)
	if r.Error != nil {
		loggrs.Error(uid, r.Error.Error())
		<-cannel
		return
	}
	shortInfo = mc

	if len(mc) < 1 {
		loggrs.Warn(uid, "目前没有账号打开短查询保障心跳！")
		j.Lark <- Body(
			&Portc{
				State:    -1,
				App:      "",
				User:     "",
				Timetr:   "",
				Comment:  "目前没有账号打开短查询保障心跳，短查询保障机制进入睡眠状态，待打开保障后才进行活跃状态。",
				Resource: nil,
				Core:     nil,
				Logfile:  "",
			})

		healthOpen.DeleteExpired()
		for _, key := range applist {
			if _, ok := healthOpen.Get(key); ok {
				healthOpen.Delete(key)
			}
		}
		// 清空分片
		applist = nil
		<-cannel
		return
	}
	for _, m2 := range mc {
		c := util.ReData{
			App:           m2.App,
			Alias:         m2.Alias,
			Username:      m2.Username,
			Password:      m2.Password,
			Ctime:         m2.Ctime,
			Init:          m2.Init,
			ResourceGroup: m2.ResourceGroup,
			Core:          m2.Core,
			Memory:        m2.Memory,
			Status:        m2.Status,
			UpdatedAt:     m2.UpdatedAt,
		}
		// 判断是否区间保障
		go j.ticker(&c, cannel)
		go j.Juct(&c, cannel)
	}
}
