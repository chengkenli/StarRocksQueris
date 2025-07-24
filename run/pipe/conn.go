/*
 *@author  chengkenli
 *@project StarRocksQuerys
 *@package app
 *@file    map
 *@date    2025/6/6 16:06
 */

package pipe

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"gorm.io/gorm"
	"sync"
)

// 全局连接池（使用 sync.Map 或普通 Map + 互斥锁）
// var syncmap sync.Map // key: 指标名（如 "adhoc"）, value: *gorm.DB
var engine *threadMap

type threadMap struct {
	Pool sync.Map
}

func newPool() *threadMap {
	return &threadMap{Pool: sync.Map{}}
}

func (engine *threadMap) Store(key any, value any) {
	engine.Pool.Store(key, value)
}

func (engine *threadMap) Load(key any) (any, bool) {
	return engine.Pool.Load(key)
}

func (engine *threadMap) Delete(key any) {
	engine.Pool.Delete(key)
}

func (engine *threadMap) setConnectMap(app string) error {
	//starrocks
	db, err := conn.StarRocks(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return err
	}
	engine.Store(app+"db", db)
	return nil
}
func (engine *threadMap) setRestyMap(app, user, password string) error {
	//resty
	restys := resty.New().SetLogger(&util.CustomLogger{}).SetBasicAuth(user, password)
	engine.Store(app+"resty", restys)
	return nil
}

func (engine *threadMap) getmapConnect(app string) (*gorm.DB, error) {
	// getConnectMap 获取指定指标的数据库连接
	if db, ok := engine.Load(app + "db"); ok {
		return db.(*gorm.DB), nil
	}
	// 如果我获取连接对象失败了，好，那我就重新设置
	util.Loggrs.Info(fmt.Sprintf("重置连接%s对象", app))
	engine._initApplication(app)
	util.Loggrs.Info(fmt.Sprintf("连接%s对象成功", app))
	return nil, errors.New("sync.Map connect db is err")
}
func (engine *threadMap) getmapResty(app string) (*resty.Client, error) {
	// getConnectMap 获取指定指标的数据库连接
	if restys, ok := engine.Load(app + "resty"); ok {
		return restys.(*resty.Client), nil
	}
	return nil, errors.New("sync.Map connect resty is err")
}

func (engine *threadMap) _initApplication(app string) {
	for _, m := range util.MetaLink {
		if m["app"].(string) == app {
			err := engine.setConnectMap(m["app"].(string))
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
			err = engine.setRestyMap(m["app"].(string), m["user"].(string), m["password"].(string))
			if err != nil {
				util.Loggrs.Error(err.Error())
			}
			break
		}
	}
}
func (engine *threadMap) _init() {
	for _, m := range util.MetaLink {
		err := engine.setConnectMap(m["app"].(string))
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
		err = engine.setRestyMap(m["app"].(string), m["user"].(string), m["password"].(string))
		if err != nil {
			util.Loggrs.Error(err.Error())
		}
	}
}

func init() {
	engine = newPool()
	//engine._init()
}
