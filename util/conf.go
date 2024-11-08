/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package util
 *@file    conf
 *@date    2024/8/7 14:42
 */

package util

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"net"
	"os"
	"path/filepath"
)

func usage() {
	fmt.Printf("\nUsage: %s [-s starrocks] [-h]\n%sStarRocks 慢查询管理\n\n", filepath.Base(os.Args[0]), filepath.Base(os.Args[0]))
	flag.PrintDefaults()
	fmt.Println()
}

func init() {
	execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	defaultConf := fmt.Sprintf("%s/.%s.yaml", execDir, filepath.Base(os.Args[0]))

	flag.StringVar(&P.ConfPath, "c", defaultConf, "conf file")
	flag.BoolVar(&P.Help, "h", false, "help")
	flag.BoolVar(&P.Check, "d", false, "debug")

	flag.Parse()
	flag.Usage = usage

	if P.Help {
		flag.Usage()
		os.Exit(-1)
	}

	paths, name := filepath.Split(P.ConfPath)
	Config = viper.New()
	Config.SetConfigFile(fmt.Sprintf("%s%s", paths, name))
	if err := Config.ReadInConfig(); err != nil {
		fmt.Println(err.Error())
	}
	LogPath = Config.GetString("logger.LogPath")
	Logrus()
}

func Parms() {
	/*获取当前主机的IP*/
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err.Error())
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				H.Ip = ipnet.IP.String()
			}
		}
	}
	//go OneStopZtyo()

}
