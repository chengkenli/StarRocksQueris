/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package StarRocksQueris
 *@file    main
 *@date    2024/8/7 14:42
 */

package main

import (
	"StarRocksQueris/run"
	"StarRocksQueris/util"
)

func main() {
	printStarRocks()
	util.Parms()
	ConfigDB()
	run.Run()
}
