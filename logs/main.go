/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package logs
 *@file    main
 *@date    2025/4/30 12:20
 */

package logs

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/rs/xid"
	"io/ioutil"
	"net/http"
	"time"
)

// Logserver
// 日志服务
func Logserver() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(keyRequestId, xid.New().String())
		c.Next()
	})

	r.GET("/log/*path", viewLog)
	r.GET("/html/*path", viewHtml)

	err := r.Run(fmt.Sprintf(":%d", 7890))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func viewLog(c *gin.Context) {
	logFile := c.Param("path")
	util.Loggrs.Info(uid, fmt.Sprintf("LOGS> %s read:[%s]", time.Now().Format("2006-01-02 15:04:05"), logFile))
	fh, err := ioutil.ReadFile(logFile)
	if err != nil {
		c.String(http.StatusBadRequest, "no log file.")
	} else {
		c.String(http.StatusOK, string(fh))
	}
}

func viewHtml(c *gin.Context) {
	logFile := c.Param("path")
	util.Loggrs.Info(uid, fmt.Sprintf("HTML> %s read:[%s]", time.Now().Format("2006-01-02 15:04:05"), logFile))
	c.File(logFile)
	return
}
