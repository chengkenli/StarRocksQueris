/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package main
 *@file    config
 *@date    2024/10/21 23:13
 */

package main

import (
	"StarRocksQueris/conn"
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"errors"
	"fmt"
	"github.com/fatih/color"
	"strings"
)

// ConfigDB 初始化连接数据库是否成功
func ConfigDB() {
	c := color.New()
	var err error
	util.Connect, err = conn.ConnectMySQL()
	if err != nil {
		util.Loggrs.Error(err)
		return
	}
	util.Loggrs.Info(c.Add(color.FgGreen).Sprint("配置数据库连接成功!"))
	initNorm()
	initRobot()
	initDB()
	util.QueryTime = int(util.ConnectNorm["slow_query_time"].(int32))
	// 验证审计表是否初始化
	authAudit()
	// 验证程序标准表是否初始化
	authStandard()
	// 验证程序集群连接信息表是否初始化
	authConnect()
	// 验证程序集群机器人信息表是否初始化
	authRobot()
	util.Loggrs.Info(c.Add(color.FgHiGreen).Sprint("读取初始化配置完成!"))
}

// 初始化Robot表
func initRobot() error {
	schema := util.Config.GetString("configdb.Schema.Robot")
	if len(schema) == 0 {
		return errors.New("robot schema is null")
	}
	r := util.Connect.Raw(fmt.Sprintf("select * from %s where status >= 1", schema)).Scan(&util.ConnectRobot)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return r.Error
	}
	return nil
}

// 初始化StarRocks登录配置
func initDB() error {
	condb := util.Config.GetString("configdb.Schema.Connect")
	if len(condb) == 0 {
		return errors.New("connect schema is null")
	}
	r := util.Connect.Raw(fmt.Sprintf("select * from %s where status >= 1", condb)).Scan(&util.ConnectBody)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return r.Error
	}
	return nil
}

// 初始化StarRocks标准配置
func initNorm() error {
	condb := util.Config.GetString("configdb.Schema.App")
	if len(condb) == 0 {
		return errors.New("app schema is null")
	}
	r := util.Connect.Raw(fmt.Sprintf("select * from %s ", condb)).Scan(&util.ConnectNorm)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return r.Error
	}
	if !tools.AuthRegis() {
		return errors.New("AuthRegis is nil")
	}

	util.ConnectLink = &util.ConnectData{
		User:     util.ConnectNorm["slow_query_data_registration_username"].(string),
		Password: util.ConnectNorm["slow_query_data_registration_password"].(string),
		Host:     util.ConnectNorm["slow_query_data_registration_host"].(string),
		Port:     int(util.ConnectNorm["slow_query_data_registration_port"].(int64)),
		Schema:   util.ConnectNorm["slow_query_data_registration_table"].(string),
	}
	return nil
}

// 验证审计日志表是否已经创建
func authAudit() {
	if !tools.AuthRegis() {
		return
	}

	conf := tools.SrAvgs{
		Host: util.ConnectNorm["slow_query_data_registration_host"].(string),
		Port: int(util.ConnectNorm["slow_query_data_registration_port"].(int64)),
		User: util.ConnectNorm["slow_query_data_registration_username"].(string),
		Pass: util.ConnectNorm["slow_query_data_registration_password"].(string),
	}
	db, err := conn.StarRocksItem(&conf)
	if err != nil {
		util.Loggrs.Error(err)
		return
	}
	tablename := util.ConnectNorm["slow_query_data_registration_table"].(string)
	split := strings.Split(tablename, ".")

	sql := fmt.Sprintf("SELECT * FROM information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", split[0], split[1])
	var m map[string]interface{}
	r := db.Raw(sql).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return
	}
	if r.RowsAffected > 0 {
		return
	}
	// 声明一个变量用来存储用户的输入
	var userInput string
	util.Loggrs.Info(fmt.Sprintf("发现慢查询数据底表[%s]并未创建，是否自动生成？！[Y/N]", tablename))
	_, err = fmt.Scanln(&userInput)
	if err != nil {
		util.Loggrs.Error("读取输入时发生错误:", err)
		return
	}
	switch userInput {
	case "Y", "y":
		// 读取用户的输入
		util.Loggrs.Info("Ok!")
		table := audit(tablename)
		util.Loggrs.Info(table)
		r := db.Exec(table)
		if r.Error != nil {
			util.Loggrs.Error(r.Error)
			return
		}
		util.Loggrs.Info(fmt.Sprintf("[%s]初始化成功！", tablename))

	case "N", "n":
		return
	default:
		util.Loggrs.Warn("输入错误！退出！")
		return
	}
}

// 验证标准配置表是否已经创建
func authStandard() {
	tablename := util.Config.GetString("configdb.Schema.App")
	if tablename == "" {
		return
	}

	split := strings.Split(tablename, ".")
	sql := fmt.Sprintf("SELECT * FROM information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", split[0], split[1])
	var m map[string]interface{}
	r := util.Connect.Raw(sql).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return
	}
	if r.RowsAffected > 0 {
		return
	}

	// 声明一个变量用来存储用户的输入
	var userInput string
	util.Loggrs.Info(fmt.Sprintf("发现慢查询数据底表[%s]并未创建，是否自动生成？！[Y/N]", tablename))
	_, err := fmt.Scanln(&userInput)
	if err != nil {
		util.Loggrs.Error("读取输入时发生错误:", err)
		return
	}
	switch userInput {
	case "Y", "y":
		// 读取用户的输入
		util.Loggrs.Info("Ok!")
		table := standard(tablename)
		util.Loggrs.Info(table)
		r := util.Connect.Exec(table)
		if r.Error != nil {
			util.Loggrs.Error(r.Error)
			return
		}
		util.Loggrs.Info(fmt.Sprintf("[%s]初始化成功！", tablename))

	case "N", "n":
		return
	default:
		util.Loggrs.Warn("输入错误！退出！")
		return
	}
}

// 验证集群连接表是否已经创建
func authConnect() {
	tablename := util.Config.GetString("configdb.Schema.Connect")
	if tablename == "" {
		return
	}

	split := strings.Split(tablename, ".")
	sql := fmt.Sprintf("SELECT * FROM information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", split[0], split[1])
	var m map[string]interface{}
	r := util.Connect.Raw(sql).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return
	}
	if r.RowsAffected > 0 {
		return
	}

	// 声明一个变量用来存储用户的输入
	var userInput string
	util.Loggrs.Info(fmt.Sprintf("发现慢查询数据底表[%s]并未创建，是否自动生成？！[Y/N]", tablename))
	_, err := fmt.Scanln(&userInput)
	if err != nil {
		util.Loggrs.Error("读取输入时发生错误:", err)
		return
	}
	switch userInput {
	case "Y", "y":
		// 读取用户的输入
		util.Loggrs.Info("Ok!")
		table := cconect(tablename)
		util.Loggrs.Info(table)
		r := util.Connect.Exec(table)
		if r.Error != nil {
			util.Loggrs.Error(r.Error)
			return
		}
		util.Loggrs.Info(fmt.Sprintf("[%s]初始化成功！", tablename))

	case "N", "n":
		return
	default:
		util.Loggrs.Warn("输入错误！退出！")
		return
	}
}

// 验证飞书机器人表是否已经创建
func authRobot() {
	tablename := util.Config.GetString("configdb.Schema.Robot")
	if tablename == "" {
		return
	}

	split := strings.Split(tablename, ".")
	sql := fmt.Sprintf("SELECT * FROM information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s'", split[0], split[1])
	var m map[string]interface{}
	r := util.Connect.Raw(sql).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error)
		return
	}
	if r.RowsAffected > 0 {
		return
	}

	// 声明一个变量用来存储用户的输入
	var userInput string
	util.Loggrs.Info(fmt.Sprintf("发现慢查询数据底表[%s]并未创建，是否自动生成？！[Y/N]", tablename))
	_, err := fmt.Scanln(&userInput)
	if err != nil {
		util.Loggrs.Error("读取输入时发生错误:", err)
		return
	}
	switch userInput {
	case "Y", "y":
		// 读取用户的输入
		util.Loggrs.Info("Ok!")
		table := crobot(tablename)
		util.Loggrs.Info(table)
		r := util.Connect.Exec(table)
		if r.Error != nil {
			util.Loggrs.Error(r.Error)
			return
		}
		util.Loggrs.Info(fmt.Sprintf("[%s]初始化成功！", tablename))

	case "N", "n":
		return
	default:
		util.Loggrs.Warn("输入错误！退出！")
		return
	}
}
