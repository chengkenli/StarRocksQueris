package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
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
