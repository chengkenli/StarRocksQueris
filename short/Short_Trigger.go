/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    short_trigger
 *@date    2024/11/22 9:40
 */

package short

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
	"time"
)

func (j *Job) trigger(db *gorm.DB, cannel chan struct{}) {
	var caches = cache.New(10*time.Second, 20*time.Second)
	tick := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-tick.C:
			now := time.Now().Add(-10 * time.Second)
			var latestUpdate time.Time
			r := db.Raw(fmt.Sprintf("select updated_at from %s order by updated_at desc limit 1", shortdb)).Scan(&latestUpdate)
			if r.Error != nil {
				loggrs.Error(uid, r.Error.Error())
				return
			}
			if latestUpdate.After(now) || latestUpdate.Equal(now) || now.Equal(latestUpdate) {
				if _, ok := caches.Get("sign"); ok {
					continue
				}
				// 表已被更新
				loggrs.Info(uid, "updated_at!")
				cannel <- struct{}{}
				loggrs.Info(uid, "recovery~")
				go j.Conmit(db, cannel)
				caches.Set("sign", latestUpdate, cache.DefaultExpiration)
			}
		}
	}
}
