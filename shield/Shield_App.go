/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package shield
 *@file    ShieldGin
 *@date    2024/11/14 20:33
 */

package shield

import (
	"StarRocksQueris/util"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/rs/xid"
	"net/http"
	"strconv"
)

// Shieldapp 路由主体框架
func Shieldapp() {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("requestId", xid.New().String())
		c.Next()
	})

	r.GET("/shield", App)

	err := r.Run(":6543")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

// App 处理请求
func App(c *gin.Context) {
	app := c.Query("shield_app")
	name := c.Query("shield_name")
	sign := c.Query("shield_channel")
	status := c.Query("status")
	if len(app) == 0 || len(name) == 0 || len(sign) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"status": "Fail", "message": "request body is nil."})
		return
	}
	util.Loggrs.Info(app, name, sign)

	atoi, _ := strconv.Atoi(sign)
	atoi2, _ := strconv.Atoi(status)

	err := SetShieldData(
		&util.Shields{
			Tablename: "chengken.sr_slow_shield",
			Data: util.Shield{
				ShieldApp:     app,
				ShieldName:    name,
				ShieldChannel: atoi,
				Status:        atoi2,
			},
		},
	)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	c.AbortWithStatusJSON(http.StatusOK, gin.H{"status": "Ok", "message": "Success."})
	return
}
