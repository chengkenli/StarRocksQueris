/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendKill
 *@date    2024/9/14 13:17
 */

package fronends

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/robot"
	"StarRocksQueris/util"
	"fmt"
	"time"
)

func Onkill(action int, app, fe, id string) {
	if util.P.Check {
		return
	}
	switch action {
	case 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13:
		kills(action, app, fe, id)
	case 2:
	}
}

func kills(action int, app, fe, id string) {
	db, err := conn.StarRocksApp(app, fe)
	if err != nil {
		util.Loggrs.Warn(err.Error())
		return
	}
	/*每次使用完，主动关闭连接数*/
	defer func() {
		sqlDB, err := db.DB()
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		sqlDB.SetMaxOpenConns(30)                  //最大连接数
		sqlDB.SetMaxIdleConns(30)                  //最大空闲连接数
		sqlDB.SetConnMaxLifetime(30 * time.Second) //空闲连接最多存活时间
		sqlDB.Close()
	}()
	pid := "kill " + id
	util.Loggrs.Info(pid)
	r := db.Exec(pid)
	if r.Error != nil {
		util.Loggrs.Warn(r.Error.Error())
		return
	}
	robot.SendFsText("kill", fmt.Sprintf("action:[%d] fe:[%s] id:[%s]", action, fe, id), "", []string{"d8dba496-74a5-4a1b-8569-c5700dbfacdf"})
}
