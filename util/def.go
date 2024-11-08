/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package util
 *@file    def
 *@date    2024/8/7 14:44
 */

package util

import (
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/gorm"
	"os"
)

func init() {
	os.Mkdir(LogPath, 0755)
}

const (
	SlowQueryDangerUser     = "svccnrpths"
	SlowQueryDangerKillTime = 360
)

var (
	Config       *viper.Viper
	Loggrs       *logrus.Logger
	Connect      *gorm.DB
	P            ArvgParms
	H            Hosts
	LogPath      string
	QueryTime    int
	Domain       []map[string]string
	ConnectRobot []map[string]interface{}
	ConnectBody  []map[string]interface{}
	ConnectNorm  map[string]interface{}
	ConnectLink  *ConnectData
)

type Hosts struct {
	Ip string
}

type ArvgParms struct {
	Help     bool
	Check    bool
	ConfPath string
}

type Process []struct {
	Id        string `bson:"Id"`
	User      string `bson:"User"`
	Host      string `bson:"Host"`
	Cluster   string `bson:"Cluster"`
	Db        string `bson:"Db"`
	Command   string `bson:"Command"`
	Time      string `bson:"Time"`
	State     string `bson:"State"`
	Info      string `bson:"Info"`
	IsPending string `bson:"IsPending"`
	Warehouse string `bson:"Warehouse"`
}

type Process2 struct {
	Id        string `bson:"Id"`
	User      string `bson:"User"`
	Host      string `bson:"Host"`
	Cluster   string `bson:"Cluster"`
	Db        string `bson:"Db"`
	Command   string `bson:"Command"`
	Time      string `bson:"Time"`
	State     string `bson:"State"`
	Info      string `bson:"Info"`
	IsPending string `bson:"IsPending"`
	Warehouse string `bson:"Warehouse"`
}
type ConnectData struct {
	User, Password string
	Host           string
	Port           int
	Schema         string
}

type EmailMain struct {
	Domain  []string
	EmailTo []string
	EmailCc []string
}

type SchemaData struct {
	Ts                string  `json:"ts"`
	App               string  `json:"app"`
	QueryId           string  `json:"queryId"`
	Origin            string  `json:"origin"`
	Domain            string  `json:"domain"`
	Owner             string  `json:"owner"`
	Action            int     `json:"action"`
	Timestamp         string  `json:"timestamp"`
	QueryType         string  `json:"queryType"`
	ClientIp          string  `json:"clientIp"`
	User              string  `json:"user"`
	AuthorizedUser    string  `json:"authorizedUser"`
	ResourceGroup     string  `json:"resourceGroup"`
	Catalog           string  `json:"catalog"`
	Db                string  `json:"db"`
	State             string  `json:"state"`
	ErrorCode         string  `json:"errorCode"`
	QueryTime         int64   `json:"queryTime"`
	ScanBytes         int64   `json:"scanBytes"`
	ScanRows          int64   `json:"scanRows"`
	ReturnRows        int64   `json:"returnRows"`
	CpuCostNs         int64   `json:"cpuCostNs"`
	MemCostBytes      int64   `json:"memCostBytes"`
	StmtId            int     `json:"stmtId"`
	IsQuery           int     `json:"isQuery"`
	FeIp              string  `json:"feIp"`
	Stmt              string  `json:"stmt"`
	Digest            string  `json:"digest"`
	PlanCpuCosts      float64 `json:"planCpuCosts"`
	PlanMemCosts      float64 `json:"planMemCosts"`
	PendingTimeMs     int64   `json:"pendingTimeMs"`
	Logfile           string  `json:"logfile"`
	Optimization      int     `json:"optimization"`
	OptimizationItems string  `json:"optimizationItems"`
}

type Emailinfo struct {
	Subject string
	To      string
	From    string
	Cc      []string
	Bc      string
	Attach  string
	Emsg    string
}

type Larkbodys struct {
	Message string
	Logfile string
}

type OlapScanExplain struct {
	OlapCount     int
	OlapScan      bool
	OlapPartition []string
}

type SortKeys struct {
	SplikKey  []string
	SortKey   []string
	SplitKeys []string
	IntArr    []string
	DecArr    []string
	DateArr   []string
	StrArr    []string
}

type SchemaSortKey struct {
	Schema  string
	SortKey *SortKeys
}

type BucketJson struct {
	App          string `json:"app"`
	Best         int    `json:"best"`
	Buckets      string `json:"buckets"`
	Client       string `json:"client"`
	Conservative int    `json:"conservative"`
	Datasize     string `json:"datasize"`
	Msg          string `json:"msg"`
	Normal       bool   `json:"normal"`
	Table        string `json:"table"`
}

type SessionWarnLark struct {
	Db           *gorm.DB
	App          string
	Fe           string
	FileLog      string
	TableList    []string
	Roboot       []string
	BucketStatus bool
	SCache       *cache.Cache
	Item         *Process2
	TfIdfs       []string
}

type SessionBigQuery struct {
	Db            *gorm.DB
	LogFile       string
	StartTime     string
	QueryId       string
	ConnectionId  string
	Database      string
	User          string
	ScanBytes     string
	ScanRows      string
	MemoryUsage   string
	DiskSpillSize string
	CPUTime       string
	ExecTime      string
	Nodes         []string
	Stmt          string
}

type InQue struct {
	Nature   string
	Opinion  string
	Sign     string
	App      string
	Fe       string
	Tbs      []string
	Rd       []string
	Item     *Process2
	Olapscan *OlapScanExplain
	Sortkey  []*SchemaSortKey
	Buckets  []string
	Logfile  string
	Normal   bool
	Queryid  []string
	Edtime   int
	Schema   []string
	Queris   *SessionBigQuery
	FsCache  *cache.Cache
	EmCache  *cache.Cache
	Avgs     []string
	Action   int
	Connect  *gorm.DB
	Iceberg  string
}

type Queris []struct {
	StartTime     string `bson:"StartTime"`
	QueryId       string `bson:"QueryId"`
	ConnectionId  string `bson:"ConnectionId"`
	Database      string `bson:"Database"`
	User          string `bson:"User"`
	ScanBytes     string `bson:"ScanBytes"`
	ScanRows      string `bson:"ScanRows"`
	MemoryUsage   string `bson:"MemoryUsage"`
	DiskSpillSize string `bson:"DiskSpillSize"`
	CPUTime       string `bson:"CPUTime"`
	ExecTime      string `bson:"ExecTime"`
	Warehouse     string `bson:"Warehouse"`
}
type Querisign struct {
	StartTime     string `bson:"StartTime"`
	QueryId       string `bson:"QueryId"`
	ConnectionId  string `bson:"ConnectionId"`
	Database      string `bson:"Database"`
	User          string `bson:"User"`
	ScanBytes     string `bson:"ScanBytes"`
	ScanRows      string `bson:"ScanRows"`
	MemoryUsage   string `bson:"MemoryUsage"`
	DiskSpillSize string `bson:"DiskSpillSize"`
	CPUTime       string `bson:"CPUTime"`
	ExecTime      string `bson:"ExecTime"`
	Warehouse     string `bson:"Warehouse"`
}
type Grafana struct {
	App          string
	Action       int
	ConnectionId string
	User         string
	Sign         string
}

type Backends []struct {
	BackendId             int    `bson:"BackendId"`
	IP                    string `bson:"IP"`
	HeartbeatPort         int    `bson:"HeartbeatPort"`
	BePort                int    `bson:"BePort"`
	HttpPort              int    `bson:"HttpPort"`
	BrpcPort              int    `bson:"BrpcPort"`
	LastStartTime         string `bson:"LastStartTime"`
	LastHeartbeat         string `bson:"LastHeartbeat"`
	Alive                 bool   `bson:"Alive"`
	SystemDecommissioned  bool   `bson:"SystemDecommissioned"`
	ClusterDecommissioned bool   `bson:"ClusterDecommissioned"`
	TabletNum             int    `bson:"TabletNum"`
	DataUsedCapacity      string `bson:"DataUsedCapacity"`
	AvailCapacity         string `bson:"AvailCapacity"`
	TotalCapacity         string `bson:"TotalCapacity"`
	UsedPct               string `bson:"UsedPct"`
	MaxDiskUsedPct        string `bson:"MaxDiskUsedPct"`
	ErrMsg                string `bson:"ErrMsg"`
	Version               string `bson:"Version"`
	Status                string `bson:"Status"`
	DataTotalCapacity     string `bson:"DataTotalCapacity"`
	DataUsedPct           string `bson:"DataUsedPct"`
	CpuCores              int    `bson:"CpuCores"`
	NumRunningQueries     int    `bson:"NumRunningQueries"`
	MemUsedPct            string `bson:"MemUsedPct"`
	CpuUsedPct            string `bson:"CpuUsedPct"`
}
