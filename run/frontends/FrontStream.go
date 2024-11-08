/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package analysis
 *@file    analysisToSchema
 *@date    2024/8/16 10:53
 */

package fronends

import (
	"StarRocksQueris/util"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"net/http"
	"strings"
	"time"
)

// SessionAnalysisToSchema 慢查询落表
func SessionAnalysisToSchema(citem *util.ConnectData, sitem *[]*util.SchemaData) {
	marshal, err := json.Marshal(sitem)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	if citem.Schema == "" {
		return
	}
	tb := strings.Split(citem.Schema, ".")

	util.Loggrs.Info("stream load...")
	//创建Resty客户端
	Client := resty.New().SetDisableWarn(true)
	//发送POST请求并处理响应

	response, err := Client.SetRedirectPolicy(resty.RedirectPolicyFunc(func(req *http.Request, via []*http.Request) error {
		// 这里你可以根据需要添加自定义逻辑，比如保留headers等
		for key, values := range via[0].Header {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		// 如果想要完全信任所有重定向，只需返回nil
		return nil
	})).R().
		SetHeaders(map[string]string{
			"label":             fmt.Sprintf("%s_%s_%d", strings.ReplaceAll(citem.Host, ".", "_"), time.Now().Format("2006_01_02_15_04_05"), time.Now().UnixMilli()), /*label*/
			"Expect":            "100-continue",                                                                                                                      /*在服务器拒绝导入作业请求的情况下，避免不必要的数据传输，减少不必要的资源开销。*/
			"format":            "json",                                                                                                                              /*导入数据的格式。取值包括 CSV 和 JSON*/
			"timezone":          "Asia/Shanghai",                                                                                                                     /*默认为东八区 (Asia/Shanghai)*/
			"max_filter_ratio":  "0",                                                                                                                                 /*指定导入作业的最大容错率 取值范围：0~1*/
			"strip_outer_array": "true",                                                                                                                              /*裁剪最外层的数组结构*/
			"ignore_json_size":  "true",                                                                                                                              /*是否检查 HTTP 请求中 JSON Body 的大小*/
		}).SetBasicAuth(citem.User, citem.Password).
		SetBody(marshal).
		Put(fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", citem.Host, citem.Port, tb[0], tb[1]))
	if err != nil {
		util.Loggrs.Error(err.Error())
		return
	}
	util.Loggrs.Info(string(response.Body()))
}
