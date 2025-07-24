/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package main
 *@file    Initialize
 *@date    2024/11/6 22:41
 */

package main

import "fmt"

// 慢查询审计日志表
func audit(tablename string) string {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
ts                date            NOT NULL COMMENT "数据载入日期",
app               varchar(64)     NOT NULL COMMENT "集群名称",
queryId           varchar(64)     NOT NULL COMMENT "查询的唯一ID",
origin            varchar(500)        NULL COMMENT "SQL原始语句中涉及到的表",
domain            varchar(64)         NULL COMMENT "主题域简称",
owner             varchar(64)         NULL COMMENT "主题域owner",
action            int(11)             NULL COMMENT "拦截行为：⓿.状态异常停留清退,①.异常违规参数查杀,②.10分钟慢查询提醒,③.30分钟慢查询查杀,④.全表扫描亿级查杀,⑤.TB级扫描字节查杀,⑥.百亿扫描行数查杀,⑦.CATALOG违规查杀,⑧.GB级消耗内存查杀",
timestamp         datetime        NOT NULL COMMENT "查询开始时间",
queryType         varchar(12)         NULL COMMENT "查询类型（query, slow_query,connection）",
clientIp          varchar(32)         NULL COMMENT "客户端IP",
user              varchar(64)         NULL COMMENT "查询用户名",
authorizedUser    varchar(64)         NULL COMMENT "用户唯一标识，既user_identity",
resourceGroup     varchar(64)         NULL COMMENT "资源组名",
catalog           varchar(32)         NULL COMMENT "数据目录名",
db                varchar(96)         NULL COMMENT "查询所在数据库",
state             varchar(8)          NULL COMMENT "查询状态（EOF，ERR，OK）",
errorCode         varchar(96)         NULL COMMENT "错误码",
queryTime         bigint(20)          NULL COMMENT "查询执行时间（秒）",
scanBytes         bigint(20)          NULL COMMENT "查询扫描的字节数",
scanRows          bigint(20)          NULL COMMENT "查询扫描的记录行数",
returnRows        bigint(20)          NULL COMMENT "查询返回的结果行数",
cpuCostNs         bigint(20)          NULL COMMENT "查询CPU耗时（纳秒）",
memCostBytes      bigint(20)          NULL COMMENT "查询消耗内存（字节）",
stmtId            int(11)             NULL COMMENT "SQL语句增量ID",
isQuery           tinyint(4)          NULL COMMENT "SQL是否为查询（1或0）",
feIp              varchar(32)         NULL COMMENT "执行该语句的FE IP",
stmt              varchar(1048576)    NULL COMMENT "SQL原始语句",
digest            varchar(32)         NULL COMMENT "慢SQL指纹",
planCpuCosts      double              NULL COMMENT "查询规划阶段CPU占用（纳秒）",
planMemCosts      double              NULL COMMENT "查询规划阶段内存占用（字节）",
pendingTimeMs     bigint(20)          NULL COMMENT "查询在队列中等待的时间（毫秒）",
logfile           varchar(200)        NULL COMMENT "日志文件",
optimization      int(11)             NULL COMMENT "是否做过优化：0.没有优化，1~99.代表优化次数",
optimizationItems varchar(1048576)    NULL COMMENT "优化项"
) ENGINE=OLAP
PRIMARY KEY(ts, app, queryId)
COMMENT "慢查询审计日志表"
PARTITION BY date_trunc('day', ts)
DISTRIBUTED BY HASH(ts, queryId)`, tablename)
	return stmt
}

func standard(tablename string) string {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  slow_query_time int NOT NULL DEFAULT '600' COMMENT '慢查询语句的超时告警时间，单位秒。',
  slow_query_ktime int NOT NULL DEFAULT '1500' COMMENT '慢查询语句的查杀时间，单位秒',
  slow_query_concurrencylimit int NOT NULL DEFAULT '80' COMMENT '慢查询的并发度（比如并发语句超过该值则告警），单位整数',
  slow_query_version varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '程序版本号',
  slow_query_focususer varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询保护白名单用户，使用英文逗号,隔开',
  slow_query_proxy_feishu varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '访问飞书代理地址(使用飞书发送信息时，企业需要代理)',
  slow_query_grafana varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'prometheus地址，支持向prometheus中推送记录',
  slow_query_lark_app varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用名称（企业版）',
  slow_query_lark_appid varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用Appid',
  slow_query_lark_appsecret varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用AppSecret',
  slow_query_email_host varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，服务器，host:port',
  slow_query_email_from varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，用于发送邮件的邮箱',
  slow_query_email_to varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，用于接收邮件的邮箱, 逗号分隔',
  slow_query_email_cc varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，由于抄送邮件给cc的邮箱，逗号分隔',
  slow_query_email_bc varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，由于密送邮件给bc的邮箱，逗号分隔',
  slow_query_email_suffix varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮件的后缀名，@xxxxx.com',
  slow_query_email_reference_material varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '邮件中呈现的参考资料了解，支持html，逗号分隔',
  slow_query_frontend_avgs varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'parallel_fragment_exec_instance_num=15,query_mem_limit=274877906944,load_mem_limit=274877906944,exec_mem_limit=274877906944' COMMENT '慢查询需要拦截的参数指标比如，key=value,.... 可填多个',
  slow_query_frontend_fullscan_num int DEFAULT '200000000' COMMENT '慢查询拦截全表扫描的最大行数，默认值2亿',
  slow_query_frontend_insert_catalog_scanrow int DEFAULT '100000000' COMMENT '慢查询拦截catalog扫描数据量超过亿级 + INSERT TABLE FROM CATALOG',
  slow_query_frontend_memoryusage int DEFAULT '200' COMMENT '慢查询拦截单个BE 200GB+级别查询消耗内存',
  slow_query_frontend_scanrows bigint DEFAULT '10000000000' COMMENT '慢查询拦截百亿+级别扫描行数',
  slow_query_frontend_scanbytes int DEFAULT '5' COMMENT '慢查询拦截TB+级别扫描字节消耗',
  slow_query_data_registration_username varchar(100) DEFAULT NULL COMMENT '慢查询记录落表，用户名',
  slow_query_data_registration_password varchar(500) DEFAULT NULL COMMENT '慢查询记录落表，密码',
  slow_query_data_registration_table varchar(500) DEFAULT NULL COMMENT '慢查询记录落表，表名',
  slow_query_data_registration_host varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，主机名（FE IP）',
  slow_query_data_registration_port int DEFAULT '8030' COMMENT '慢查询记录落表，端口(因为这个走的是stream load，所以端口默认8030)',
  slow_query_resource_group_cpu_core_limit int DEFAULT '10' COMMENT '慢查询拦截资源隔离，CPU',
  slow_query_resource_group_mem_limit int DEFAULT '50' COMMENT '慢查询拦截资源隔离，内存',
  slow_query_resource_group_concurrency_limit int DEFAULT '3' COMMENT '慢查询拦截资源隔离，并发度',
  updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`, tablename)
	return stmt
}

func cconect(tablename string) string {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  app           varchar(100)    NOT NULL COMMENT '集群名称(英文)',
  feip          varchar(200)    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群连接地址(必填)F5,VIP,CLB,FE',
  user          varchar(200)    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群登录账号(必填) 建议是管理员角色的账号',
  password      varchar(500)    NOT NULL COMMENT '集群登录密码(必填)',
  feport        int             NOT NULL DEFAULT '9030' COMMENT '集群登录端口，默认9030',
  address       varchar(500)    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'MANAGER地址，如果填了MANAGER地址，那么将触发定时检查LICENSE是否过期(企业级)',
  expire        int             DEFAULT '30' COMMENT 'LICENSE是否过期(企业级)过期提醒倒计时，单位day',
  status        int             NOT NULL DEFAULT '0' COMMENT 'LICENSE是否过期(企业级)开关,0 off, 1 on',
  updated_at    timestamp       NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='StarRocks登录配置，manager地址,(定期检查license过期日期)'`, tablename)
	return stmt
}

func crobot(tablename string) string {
	stmt := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  type          varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人类型，global,cluster,user',
  key           varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人集群通知标记',
  robot         varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书机器人KEY',
  status        int NOT NULL DEFAULT '0' COMMENT '开关',
  updated_at    timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='慢查询告警推送机器人'`, tablename)
	return stmt
}
