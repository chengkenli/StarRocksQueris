/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package StarRocksQueris
 *@file    main
 *@date    2024/8/7 14:42
 */

package main

import (
	"StarRocksQueris/logs"
	"StarRocksQueris/run"
	"StarRocksQueris/util"
)

func main() {
	go logs.Logserver()
	printStarRocks()
	util.Parms()
	ConfigDB()
	run.Run()
}
