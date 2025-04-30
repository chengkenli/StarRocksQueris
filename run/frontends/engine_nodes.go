package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"time"
)

type Fronends []struct {
	Name              string `bson:"Name"`
	IP                string `bson:"IP"`
	EditLogPort       int    `bson:"EditLogPort"`
	HttpPort          int    `bson:"HttpPort"`
	QueryPort         int    `bson:"QueryPort"`
	RpcPort           int    `bson:"RpcPort"`
	Role              string `bson:"Role"`
	IsMaster          string `bson:"IsMaster"`
	ClusterId         int    `bson:"ClusterId"`
	Join              string `bson:"Join"`
	Alive             string `bson:"Alive"`
	ReplayedJournalId int    `bson:"ReplayedJournalId"`
	LastHeartbeat     string `bson:"LastHeartbeat"`
	IsHelper          string `bson:"IsHelper"`
	ErrMsg            string `bson:"ErrMsg"`
	StartTime         string `bson:"StartTime"`
	Version           string `bson:"Version"`
}

func FronendNodes(app string) []string {
	db, err := conn.StarRocks(app)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	var (
		f Fronends
	)
	var a []string
	db.Raw("show frontends").Scan(&f)
	for _, s := range f {
		a = append(a, s.IP)
	}
	return a
}
