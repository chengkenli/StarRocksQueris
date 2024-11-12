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
	"gorm.io/gorm"
	"strings"
)

func MetaData(app, userid string) (string, string, []string) {
	// 链接到集群
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(err.Error())
		return "", "", nil
	}
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
		owner, direct = seriId(db, app, userid)
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

func seriId(db *gorm.DB, app, userid string) (string, []string) {
	var m map[string]interface{}
	r := db.Raw(fmt.Sprintf("select * from ops.datalake_account_information where cluster_sort_name='%s' and account='%s'", app, userid)).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return "", nil
	}
	if m == nil {
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
