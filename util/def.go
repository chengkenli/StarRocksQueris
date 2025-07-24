/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package util
 *@file    def
 *@date    2024/8/7 14:44
 */

package util

import (
	"StarRocksQueris/pool"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/gorm"
	"os"
	"time"
)

func init() {
	os.Mkdir(LogPath, 0755)
}

const (
	SlowQueryDangerUser     = "svccnrpths"
	SlowQueryDangerKillTime = 360
	ENCKEY                  = "xxx"
)

var (
	Config       *viper.Viper
	Loggrs       *logrus.Logger
	Connect      *gorm.DB
	P            ArvgParms
	H            Hosts
	LogPath      string
	Domain       []map[string]string
	ConnectRobot []map[string]interface{}
	ConnectBody  []map[string]interface{}
	ConnectNorm  *DBConfig
	ConnectLink  *ConnectData
	ClientIPDec  []ClientIPData
	MetaLink     []map[string]interface{}
)

type CustomLogger struct{}

func (l *CustomLogger) Errorf(format string, v ...interface{}) {}
func (l *CustomLogger) Warnf(format string, v ...interface{})  {} // 忽略 WARN
func (l *CustomLogger) Debugf(format string, v ...interface{}) {}

type Hosts struct {
	Ip string
}

type ArvgParms struct {
	Help     bool
	Check    bool
	ConfPath string
}

type DBConfig struct {
	SlowQueryTime                          int       `bson:"slow_query_time"`
	SlowQueryKtime                         int       `bson:"slow_query_ktime"`
	SlowQueryConcurrencylimit              int       `bson:"slow_query_concurrencylimit"`
	SlowQueryVersion                       string    `bson:"slow_query_version"`
	SlowQueryFocususer                     string    `bson:"slow_query_focususer"`
	SlowQueryProxyFeishu                   string    `bson:"slow_query_proxy_feishu"`
	SlowQueryGrafana                       string    `bson:"slow_query_grafana"`
	SlowQueryLarkApp                       string    `bson:"slow_query_lark_app"`
	SlowQueryLarkAppid                     string    `bson:"slow_query_lark_appid"`
	SlowQueryLarkAppsecret                 string    `bson:"slow_query_lark_appsecret"`
	SlowQueryEmailHost                     string    `bson:"slow_query_email_host"`
	SlowQueryEmailFrom                     string    `bson:"slow_query_email_from"`
	SlowQueryEmailTo                       string    `bson:"slow_query_email_to"`
	SlowQueryEmailCc                       string    `bson:"slow_query_email_cc"`
	SlowQueryEmailBc                       string    `bson:"slow_query_email_bc"`
	SlowQueryEmailSuffix                   string    `bson:"slow_query_email_suffix"`
	SlowQueryEmailReferenceMaterial        string    `bson:"slow_query_email_reference_material"`
	SlowQueryFrontendAvgs                  string    `bson:"slow_query_frontend_avgs"`
	SlowQueryFrontendFullscanNum           int       `bson:"slow_query_frontend_fullscan_num"`
	SlowQueryFrontendInsertCatalogScanrow  int       `bson:"slow_query_frontend_insert_catalog_scanrow"`
	SlowQueryFrontendMemoryusage           int       `bson:"slow_query_frontend_memoryusage"`
	SlowQueryFrontendScanrows              int64     `bson:"slow_query_frontend_scanrows"`
	SlowQueryFrontendScanbytes             int       `bson:"slow_query_frontend_scanbytes"`
	SlowQueryDataRegistrationUsername      string    `bson:"slow_query_data_registration_username"`
	SlowQueryDataRegistrationPassword      string    `bson:"slow_query_data_registration_password"`
	SlowQueryDataRegistrationTable         string    `bson:"slow_query_data_registration_table"`
	SlowQueryDataRegistrationHost          string    `bson:"slow_query_data_registration_host"`
	SlowQueryDataRegistrationPort          int       `bson:"slow_query_data_registration_port"`
	SlowQueryResourceGroupCpuCoreLimit     int       `bson:"slow_query_resource_group_cpu_core_limit"`
	SlowQueryResourceGroupMemLimit         int       `bson:"slow_query_resource_group_mem_limit"`
	SlowQueryResourceGroupConcurrencyLimit int       `bson:"slow_query_resource_group_concurrency_limit"`
	SlowQueryMetaapp                       string    `bson:"slow_query_metaapp"`
	SlowQueryAuditload                     string    `bson:"slow_query_auditload"`
	UpdatedAt                              time.Time `bson:"updated_at"`
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
	App     string
	Message string
	Logfile string
	Action  int
	Button  string
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
	Nature     string
	Opinion    string
	Sign       string
	App        string
	Fe         string
	Tbs        []string
	Rd         []string
	Item       *Process2
	Olapscan   *OlapScanExplain
	Sortkey    []*SchemaSortKey
	Buckets    []string
	Logfile    string
	Normal     bool
	Queryid    []string
	Edtime     int
	Schema     []string
	Queris     *SessionBigQuery
	Larkcache  *pool.CacheWrapper
	Emailcache *pool.CacheWrapper
	Avgs       []string
	Action     int
	Connect    *gorm.DB
	Iceberg    string
	Explog     string
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

type Shields struct {
	Tablename string
	Data      Shield
}
type ShieldUrls struct {
	Tablename string
	Data      ShieldUrl
}

type Shield struct {
	ShieldApp     string `bson:"shield_app"`
	ShieldName    string `bson:"shield_name"`
	ShieldChannel int    `bson:"shield_channel"`
	Status        int    `bson:"status"`
}

type ShieldUrl struct {
	ShieldApp     string `bson:"shield_app"`
	ShieldName    string `bson:"shield_name"`
	ShieldChannel int    `bson:"shield_channel"`
	ShieldRequert string `bson:"shield_requert"`
}

type ConnectDB struct {
	App      string
	User     string
	Password string
	Ctime    string
	Init     int
}

// ReData
// 单例
type ReData struct {
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

type ResultData struct {
	SortReport   []string
	OlapSchema   []string
	OlapTables   []string
	OlapScan     *OlapScanExplain
	ExplainFile  string
	SortKeys     []*SchemaSortKey
	BucketResult []string
	BucketType   bool
	QueryIds     []string
	OlapView     []string
}

type ClientIPData struct {
	Ts              string `bson:"ts"`
	ComputerName    string `bson:"computer_name"`
	UserName        string `bson:"user_name"`
	ComputerType    string `bson:"computer_type"`
	ComputerStatus  string `bson:"computer_status"`
	IpAddress       string `bson:"ip_address"`
	SerialNumber    string `bson:"serial_number"`
	Brand           string `bson:"brand"`
	Model           string `bson:"model"`
	ComputerVersion string `bson:"computer_version"`
	BusinessUnit    string `bson:"business_unit"`
	BusinessFormat  string `bson:"business_format"`
	DataSource      string `bson:"data_source"`
	LastUpdateTime  string `bson:"last_update_time"`
	AiTime          string `bson:"ai_time"`
	AddTime         string `bson:"add_time"`
	LastActiveTime  string `bson:"last_active_time"`
}

type GlobalQueries struct {
	StartTime     string `json:"StartTime"`
	FeIp          string `json:"FeIp"`
	QueryId       string `json:"QueryId"`
	ConnectionId  int64  `json:"ConnectionId"`
	Database      string `json:"Database"`
	User          string `json:"User"`
	ScanBytes     string `json:"ScanBytes"`
	ScanRows      string `json:"ScanRows"`
	MemoryUsage   string `json:"MemoryUsage"`
	DiskSpillSize string `json:"DiskSpillSize"`
	CPUTime       string `json:"CPUTime"`
	ExecTime      string `json:"ExecTime"`
	Warehouse     string `json:"Warehouse"`
	CustomQueryId string `json:"CustomQueryId"`
	ResourceGroup string `json:"ResourceGroup"`
}
