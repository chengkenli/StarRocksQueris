/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package explain
 *@file    explainBucket
 *@date    2024/8/20 19:09
 */

package explain

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"encoding/json"
	"fmt"
	"strings"
)

func GetBuckets(app string, olap []string) ([]string, bool, error) {
	if olap == nil {
		return nil, true, nil
	}
	var m []string
	normal := true
	for _, table := range olap {
		url := fmt.Sprintf(`{"app":"%s","table":"%s"}`, app, table)
		r := tools.Post("POST", fmt.Sprintf("http://%s:8855/getbuckets", util.H.Ip), strings.NewReader(url))
		m = append(m, string(r))

		type bucket struct {
			App          string `json:"app"`
			Best         int    `json:"best"`
			Buckets      string `json:"buckets"`
			Client       string `json:"client"`
			Conservative int    `json:"conservative"`
			Datasize     string `json:"datasize"`
			Msg          string `json:"msg"`
			Normal       bool   `json:"normal"`
			Table        string `json:"table"`
		}
		var b bucket
		json.Unmarshal(r, &b)
		if !b.Normal {
			normal = false
		}
	}
	return m, normal, nil
}
