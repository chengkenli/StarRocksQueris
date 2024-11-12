package conn

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"errors"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"time"
)

func StarRocks(app string) (*gorm.DB, error) {
	var avg tools.SrAvgs
	// connect
	if len(tools.UniqueMaps(util.ConnectBody)) == 0 {
		return nil, errors.New("config db is null")
	}
	for _, m := range tools.UniqueMaps(util.ConnectBody) {
		if m["app"].(string) == app {
			avg = tools.SrAvgs{
				Host: m["feip"].(string),
				Port: int(m["feport"].(int32)),
				User: m["user"].(string),
				Pass: m["password"].(string),
			}
		}
	}

	if avg.Host == "" {
		return nil, errors.New("avg is null")
	}

	newLogger := logger.New(nil,
		logger.Config{
			SlowThreshold: time.Second * 1000, // 控制慢SQL阈值
		},
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?parseTime=true&charset=utf8mb4&loc=Local",
		avg.User,
		avg.Pass,
		avg.Host,
		avg.Port,
	)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: newLogger,
	})
	if err != nil {
		util.Loggrs.Error(fmt.Sprintf("%s", err))
	}
	return db, err
}

func StarRocksApp(app, host string) (*gorm.DB, error) {
	var avg tools.SrAvgs

	if len(host) == 0 {
		return nil, errors.New("登录信息有误，或为空。")
	}
	for _, m := range tools.UniqueMaps(util.ConnectBody) {
		if m["app"].(string) == app {
			avg = tools.SrAvgs{
				Host: m["feip"].(string),
				Port: int(m["feport"].(int32)),
				User: m["user"].(string),
				Pass: m["password"].(string),
			}
		}
	}

	if avg.Host == "" {
		return nil, errors.New("avg is null")
	}

	newLogger := logger.New(nil,
		logger.Config{
			SlowThreshold: time.Second * 1000, // 控制慢SQL阈值
		},
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?charset=utf8mb4&parseTime=True&loc=Local", avg.User, avg.Pass, host, avg.Port)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名

		},
		Logger: newLogger,
	})
	if err != nil {
		util.Loggrs.Error(err.Error())
		return db, err
	}
	return db, err
}

func StarRocksItem(item *tools.SrAvgs) (*gorm.DB, error) {
	newLogger := logger.New(nil,
		logger.Config{
			SlowThreshold: time.Second * 1000, // 控制慢SQL阈值
		},
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?charset=utf8mb4&parseTime=True&loc=Local", item.User, item.Pass, item.Host, 9030)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名

		},
		Logger: newLogger,
	})
	if err != nil {
		util.Loggrs.Error(err.Error())
		return db, err
	}
	return db, err
}
