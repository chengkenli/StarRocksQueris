/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    def
 *@date    2025/1/9 17:30
 */

package short

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/sirupsen/logrus"
	"strconv"
	"time"
)

var loggrs *logrus.Logger

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
)

type prometheusData struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
				ClusterID   string `json:"cluster_id"`
				ClusterType string `json:"cluster_type"`
				Dept        string `json:"dept"`
				Env         string `json:"env"`
				Instance    string `json:"instance"`
				Job         string `json:"job"`
				JobOwner    string `json:"job_owner"`
				Module      string `json:"module"`
				Name        string `json:"name"`
				Owner       string `json:"owner"`
				TenantID    string `json:"tenant_id"`
				User        string `json:"user"`
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

type Portc struct {
	State    int
	App      string
	User     string
	Timetr   string
	Comment  string
	Resource []string
	Core     []string
	ProceBar string
	ProceVal float64
	Logfile  string
	SrcData  *util.ReData
}

type Job struct {
	Lark   chan *util.Larkbodys
	Donec  chan string
	Signal chan string
}

// ReDatas
// 多例
type ReDatas []struct {
	App           string    `bson:"app"`
	Alias         string    `bson:"alias"`
	Username      string    `bson:"username"`
	Password      string    `bson:"password"`
	Ctime         string    `bson:"ctime"`
	Init          int       `bson:"init"`
	ResourceGroup string    `bson:"resource_group"`
	Core          int       `bson:"core"`
	Memory        int       `bson:"memory"`
	Status        int       `bson:"status"`
	UpdatedAt     time.Time `bson:"updated_at"`
}

type HtmlData struct {
	Id           int
	User         string
	CpuCostNs    int64
	MemCostBytes int64
	QueryId      string
	QueryTime    int64
	ReturnRows   int64
	ScanBytes    int64
	ScanRows     int64
	Timestamp    string
	Stmt         string
}

// ByteSizeToString 将字节数转换为人类可读的字符串表示形式（如 KB、MB、GB）
func ByteSizeToString(s int64) string {
	size, _ := strconv.ParseFloat(strconv.FormatInt(s, 10), 64)
	if size < KB {
		return fmt.Sprintf("%.2fB", size)
	} else if size < MB {
		return fmt.Sprintf("%.2fKB", size/KB)
	} else if size < GB {
		return fmt.Sprintf("%.2fMB", size/MB)
	} else if size < TB {
		return fmt.Sprintf("%.2fGB", size/GB)
	} else if size < PB {
		return fmt.Sprintf("%.2fTB", size/TB)
	} else {
		return fmt.Sprintf("%.2fPB", size/PB)
	}
}
