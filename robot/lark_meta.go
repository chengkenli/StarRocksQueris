/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    meta
 *@date    2024/9/25 16:50
 */

package robot

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"strings"
	"time"
)

func MetaData(app, userid string) (string, string, []string) {
	// 链接到集群
	db, err := conn.StarRocks(app)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
		return "", "", nil
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	var mm map[string]interface{}
	r := db.Raw("SHOW AUTHENTICATION FOR " + userid).Scan(&mm)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return "", "", nil
	}
	if mm["AuthPlugin"] == nil {
		return "", "", nil
	}

	var owner, tc string
	var direct []string
	switch mm["AuthPlugin"].(string) {
	case "MYSQL_NATIVE_PASSWORD":
		tc = "native"
		owner, direct = seriId(app, userid)
	case "AUTHENTICATION_LDAP_SIMPLE":
		if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
			owner = fmt.Sprintf("%s%s", userid, util.ConnectNorm.SlowQueryEmailSuffix)
		} else {
			owner = userid
		}
		tc = "ldap"
	}
	return tc, owner, direct
}

func seriId(app, userid string) (string, []string) {
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
		return "", nil
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(uid, err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	var m map[string]interface{}
	r := db.Raw(fmt.Sprintf("select * from ops.datalake_account_information where cluster_sort_name='%s' and account='%s'", app, userid)).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return "", nil
	}
	if m == nil || m["user_id"] == nil {
		util.Loggrs.Warn("r is nil.")
		return "", nil
	}
	var to string
	if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
		to = m["user_id"].(string) + util.ConnectNorm.SlowQueryEmailSuffix
	} else {
		to = m["user_id"].(string)
	}

	var cc []string
	if len(m["direct_reports"].(string)) != 0 {
		for _, key := range strings.Split(m["direct_reports"].(string), ",") {
			if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
				cc = append(cc, strings.Split(key, ":")[1]+util.ConnectNorm.SlowQueryEmailSuffix)
			} else {
				cc = append(cc, strings.Split(key, ":")[1])
			}
		}
	}
	return to, tools.RemoveDuplicateStrings(cc)
}
