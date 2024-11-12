-- chengken.sr_slow_query_manager definition

CREATE TABLE `sr_slow_query_manager` (
  `app` varchar(100) NOT NULL COMMENT '集群名称(英文)',
  `feip` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群连接地址(必填)F5,VIP,CLB,FE',
  `user` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集群登录账号(必填) 建议是管理员角色的账号',
  `password` varchar(500) NOT NULL COMMENT '集群登录密码(必填)',
  `feport` int NOT NULL DEFAULT '9030' COMMENT '集群登录端口，默认9030',
  `address` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'MANAGER地址，如果填了MANAGER地址，那么将触发定时检查LICENSE是否过期(企业级)',
  `expire` int DEFAULT '30' COMMENT 'LICENSE是否过期(企业级)过期提醒倒计时，单位day',
  `status` int NOT NULL DEFAULT '0' COMMENT 'LICENSE是否过期(企业级)开关,0 off, 1 on',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='StarRocks登录配置，manager地址,(定期检查license过期日期)';



-- chengken.sr_slow_query_robot definition

CREATE TABLE `sr_slow_query_robot` (
  `type` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人类型，global,cluster,user',
  `key` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '机器人集群通知标记',
  `robot` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '飞书机器人KEY',
  `status` int NOT NULL DEFAULT '0' COMMENT '开关',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='慢查询告警推送机器人';


-- chengken.sr_slow_query_config definition

CREATE TABLE `sr_slow_query_config` (
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
  `slow_query_data_registration_username` varchar(100) DEFAULT NULL COMMENT '慢查询记录落表，用户名',
  `slow_query_data_registration_password` varchar(500) DEFAULT NULL COMMENT '慢查询记录落表，密码',
  `slow_query_data_registration_table` varchar(500) DEFAULT NULL COMMENT '慢查询记录落表，表名',
  `slow_query_data_registration_host` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '慢查询记录落表，主机名（FE IP）',
  `slow_query_data_registration_port` int DEFAULT '8030' COMMENT '慢查询记录落表，端口(因为这个走的是stream load，所以端口默认8030)',
  `slow_query_resource_group_cpu_core_limit` int DEFAULT '10' COMMENT '慢查询拦截资源隔离，CPU',
  `slow_query_resource_group_mem_limit` int DEFAULT '50' COMMENT '慢查询拦截资源隔离，内存',
  `slow_query_resource_group_concurrency_limit` int DEFAULT '3' COMMENT '慢查询拦截资源隔离，并发度',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;