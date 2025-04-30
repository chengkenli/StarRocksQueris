/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package robot
 *@file    func
 *@date    2025/1/24 16:49
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"github.com/patrickmn/go-cache"
	"time"
)

var robotCache = cache.New(10*time.Minute, 10*time.Minute)
var robotChan = make(chan string)

func init() {

	go func() {
		for {
			select {
			case kkey := <-robotChan:
				util.Loggrs.Info(uid, "获取robot cache >", kkey)
				if _, ok := robotCache.Get(kkey); !ok {
					for _, m := range tools.UniqueMaps(util.ConnectRobot) {
						if m["type"] != nil {
							if m["type"].(string) != "cluster" {
								continue
							}
							if m["key"] == nil {
								continue
							}
							if m["robot"] == nil {
								continue
							}
							app := m["key"].(string)
							robot := m["robot"].(string)

							robotCache.Set(app, robot, cache.DefaultExpiration)
						}
					}
				}
			}
		}
	}()
}
