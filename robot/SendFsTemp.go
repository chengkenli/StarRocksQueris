/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package robot
 *@file    SendFsTemp
 *@date    2024/10/31 13:36
 */

package robot

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/util"
	"errors"
	"fmt"
	"gorm.io/gorm"
)

// UidresUlt 根据userid从数据表中匹配openid
func UidresUlt(userid string, db *gorm.DB) (string, string, error) {
	var mc map[string]interface{}
	r := db.Raw(fmt.Sprintf("select * from ops.feishu_openid where userid='%s'", userid)).Scan(&mc)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return "", "", errors.New(r.Error.Error())
	}
	if mc == nil {
		return "", "", errors.New("根据userid从数据表中匹配openid，[] is nil")
	}
	if mc["openid"] == nil {
		return "", "", errors.New("根据userid从数据表中匹配openid，[]openid is nil")
	}
	util.Loggrs.Info("匹配成功！")
	return mc["username"].(string), mc["openid"].(string), nil
}

func updateOpenID(userid, openid string) {
	// 链接到集群
	db, err := conn.StarRocks("sr-adhoc")
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	sql := fmt.Sprintf("update ops.feishu_openid set `userid` = '%s' where openid='%s'", userid, openid)
	util.Loggrs.Info(sql)
	r := db.Exec(sql)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return
	}
	util.Loggrs.Info(fmt.Sprintf("update done. userid:[%s],openid:[%s]", userid, openid))
}
