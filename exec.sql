-- chengken.starrocks_information_connections definition

CREATE TABLE `starrocks_information_connections` (
  `app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群名称(英文)',
  `nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '别名',
  `alias` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '集群别名',
  `feip` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群连接地址(必填)F5,VIP,CLB,FE',
  `user` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群登录账号(必填) 建议是管理员角色的账号',
  `password` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群登录密码(必填)',
  `feport` int NOT NULL DEFAULT '9030' COMMENT '集群登录端口，默认9030',
  `address` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'MANAGER地址，如果填了MANAGER地址，那么将触发定时检查LICENSE是否过期(企业级)',
  `expire` int DEFAULT '30' COMMENT 'LICENSE是否过期(企业级)过期提醒倒计时，单位day',
  `status` int NOT NULL DEFAULT '0' COMMENT 'LICENSE是否过期(企业级)开关,0 off, 1 on',
  `fe_log_path` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'FE 日志目录',
  `be_log_path` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'BE 日志目录',
  `java_udf_path` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'BE 日志目录',
  `manager_access_key` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'manager 开发者的access key',
  `manager_secret_key` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'manager 开发者的secret key',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='StarRocks登录配置，manager地址,(定期检查license过期日期)';


-- chengken.starrocks_information_larkrobot definition

CREATE TABLE `starrocks_information_larkrobot` (
  `type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人类型，global,cluster,user',
  `key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人集群通知标记',
  `robot` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书机器人KEY',
  `status` int NOT NULL DEFAULT '0' COMMENT '开关',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='慢查询告警推送机器人';


-- chengken.starrocks_information_shortconfig definition

CREATE TABLE `starrocks_information_shortconfig` (
  `app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群标识',
  `alias` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '集群别名',
  `username` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '登录账号',
  `password` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '密码',
  `ctime` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '需要保护的时间段eg.[09:00-18:00]',
  `init` int NOT NULL DEFAULT '0' COMMENT '初始化启动时，是否推送信息，0不需要，1需要',
  `resource_group` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '资源组名称',
  `core` int DEFAULT NULL COMMENT 'cpu core',
  `memory` int DEFAULT NULL COMMENT '内存/GB',
  `status` int NOT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- chengken.starrocks_information_slowconfig definition

CREATE TABLE `starrocks_information_slowconfig` (
  `slow_query_time` int NOT NULL DEFAULT '600' COMMENT '慢查询语句的超时告警时间，单位秒。',
  `slow_query_ktime` int NOT NULL DEFAULT '1500' COMMENT '慢查询语句的查杀时间，单位秒',
  `slow_query_concurrencylimit` int NOT NULL DEFAULT '80' COMMENT '慢查询的并发度（比如并发语句超过该值则告警），单位整数',
  `slow_query_version` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '程序版本号',
  `slow_query_focususer` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询保护白名单用户，使用英文逗号,隔开',
  `slow_query_proxy_feishu` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '访问飞书代理地址(使用飞书发送信息时，企业需要代理)',
  `slow_query_grafana` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'prometheus地址，支持向prometheus中推送记录',
  `slow_query_lark_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用名称（企业版）',
  `slow_query_lark_appid` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用Appid',
  `slow_query_lark_appsecret` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书应用AppSecret',
  `slow_query_email_host` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，服务器，host:port',
  `slow_query_email_from` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，用于发送邮件的邮箱',
  `slow_query_email_to` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，用于接收邮件的邮箱, 逗号分隔',
  `slow_query_email_cc` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，由于抄送邮件给cc的邮箱，逗号分隔',
  `slow_query_email_bc` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮箱，由于密送邮件给bc的邮箱，逗号分隔',
  `slow_query_email_suffix` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '企业邮件的后缀名，@xxxxx.com',
  `slow_query_email_reference_material` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '邮件中呈现的参考资料了解，支持html，逗号分隔',
  `slow_query_frontend_avgs` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT 'parallel_fragment_exec_instance_num=15,query_mem_limit=274877906944,load_mem_limit=274877906944,exec_mem_limit=274877906944' COMMENT '慢查询需要拦截的参数指标比如，key=value,.... 可填多个',
  `slow_query_frontend_fullscan_num` int DEFAULT '200000000' COMMENT '慢查询拦截全表扫描的最大行数，默认值2亿',
  `slow_query_frontend_insert_catalog_scanrow` int DEFAULT '100000000' COMMENT '慢查询拦截catalog扫描数据量超过亿级 + INSERT TABLE FROM CATALOG',
  `slow_query_frontend_memoryusage` int DEFAULT '200' COMMENT '慢查询拦截单个BE 200GB+级别查询消耗内存',
  `slow_query_frontend_scanrows` bigint DEFAULT '10000000000' COMMENT '慢查询拦截百亿+级别扫描行数',
  `slow_query_frontend_scanbytes` int DEFAULT '5' COMMENT '慢查询拦截TB+级别扫描字节消耗',
  `slow_query_data_registration_username` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，用户名',
  `slow_query_data_registration_password` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，密码',
  `slow_query_data_registration_table` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，表名',
  `slow_query_data_registration_host` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，主机名（FE IP）',
  `slow_query_data_registration_port` int DEFAULT '8030' COMMENT '慢查询记录落表，端口(因为这个走的是stream load，所以端口默认8030)',
  `slow_query_resource_group_cpu_core_limit` int DEFAULT '10' COMMENT '慢查询拦截资源隔离，CPU',
  `slow_query_resource_group_mem_limit` int DEFAULT '50' COMMENT '慢查询拦截资源隔离，内存',
  `slow_query_resource_group_concurrency_limit` int DEFAULT '3' COMMENT '慢查询拦截资源隔离，并发度',
  `slow_query_metaapp` varchar(100) DEFAULT NULL COMMENT '元数据的集群名称',
  `slow_query_auditload` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '集成审计日志auditload的表名(开启短查询保障需要)',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 内部人员飞书元数据信息表
-- 这里面记录是公司员工的userid(员工id)，openid(飞书openid)，username(用户名/中英)
-- 主要是open_id，例如这里有一条记录
/*
user_id: vnshd02
open_id: ou_xxx
user_name: 张三丰
union_id: on_xxx
is_activated: 1
*/
-- 那么当vnshd02这个账号提交的慢查询被拦截后，程序会主动的通过飞书的open_id给他发一个告警信息
-- 当信息为空，则忽略
CREATE TABLE `feishu_user_information` (
  `user_id`      varchar(500) NOT NULL COMMENT "员工工号",
  `open_id`      varchar(500) NOT NULL COMMENT "飞书ID",
  `user_name`    varchar(500)     NULL COMMENT "飞书用户名称",
  `union_id`     varchar(500)     NULL COMMENT "飞书Union ID",
  `is_activated` boolean      NOT NULL COMMENT "飞书账户状态"
) ENGINE=OLAP
PRIMARY KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 2;

-- 内部提交语句的访问地址元数据表
-- 这里面记录了所有的服务器地址，员工电脑的IP
-- 假如有一条记录
/*
              ip: 172.10.1.20
            user: vnshd02
     system_name: <域名>.
    console_user: vnshd02
    manufacturer: Dell Inc.
           model: OptiPlex 3070
operating_system: Microsoft Windows 11 企业版
       timestamp: 2025-02-10 08:39:53
*/
-- 那么当vnshd02这位用户，他使用test这个账号提交了一个慢查询被拦截后，那么组合信息时，
-- 将会采集内表中的数据进行过滤，精准的显示vnshd02，使用Dell品牌的电脑，
-- 在xxx时间点，在172.10.1.20这个客户端上提交了慢查询
CREATE TABLE `ops_starrocks_ip_system` (
  `ip`                varchar(100) NOT NULL COMMENT "IP",
  `user`              varchar(100)     NULL COMMENT "用户",
  `system_name`       varchar(200)     NULL COMMENT "主机名称",
  `console_user`      varchar(100)     NULL COMMENT "域",
  `manufacturer`      varchar(100)     NULL COMMENT "制造商",
  `model`             varchar(200)     NULL COMMENT "机型",
  `operating_system`  varchar(200)     NULL COMMENT "系统",
  `timestamp`         varchar(100) NOT NULL COMMENT "数据载入时间"
) ENGINE=OLAP
PRIMARY KEY(`ip`)
COMMENT "starrocks client ip信息关系表"
DISTRIBUTED BY HASH(`ip`) BUCKETS 10