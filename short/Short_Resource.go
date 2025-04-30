/*
 *@author  chengkenli
 *@project StarRocksRM
 *@package app
 *@file    regroup
 *@date    2024/11/20 21:20
 */

package short

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type Data []struct {
	Name                   string      `bson:"name"`
	Id                     int         `bson:"id"`
	CpuCoreLimit           int         `bson:"cpu_core_limit"`
	MemLimit               string      `bson:"mem_limit"`
	MaxCpuCores            interface{} `bson:"max_cpu_cores"`
	BigQueryCpuSecondLimit interface{} `bson:"big_query_cpu_second_limit"`
	BigQueryScanRowsLimit  interface{} `bson:"big_query_scan_rows_limit"`
	BigQueryMemLimit       interface{} `bson:"big_query_mem_limit"`
	ConcurrencyLimit       interface{} `bson:"concurrency_limit"`
	SpillMemLimitThreshold interface{} `bson:"spill_mem_limit_threshold"`
	Type                   string      `bson:"type"`
	Classifiers            string      `bson:"classifiers"`
}

var srcData, srcCore []string

func Resource(c *util.ReData, db *gorm.DB) {

	var resourceName string
	for _, s := range shortInfo {
		if s.App == c.App {
			resourceName = s.ResourceGroup
		}
	}

	var data, core []string
	var d Data
	r := db.Raw("SHOW RESOURCE GROUP " + resourceName).Scan(&d)
	if r.Error != nil {
		loggrs.Warn(uid, r.Error.Error())
		return
	}

	var htmltr []string
	var wg sync.WaitGroup
	for i, i2 := range d {
		wg.Add(1)
		i2 := i2
		go func(i int) {
			defer wg.Done()
			user, queryType := regex(i2.Classifiers)
			request, millis, second, minute, hd := sumc(db, user)
			if hd != nil {
				//htmltr = append(htmltr, html_br(user))
				htmltr = append(htmltr, html_tr(hd, user)...)
			}
			//htmldata = append(htmldata, hd...)
			last5min.Set(user, user, cache.DefaultExpiration)
			data = append(data, fmt.Sprintf(`#%d>transactions:%2d millis:%2d second:%2d minute:%2d %s\n`,
				i,
				request, millis, second, minute,
				user,
			))
			memlimit, _ := strconv.ParseFloat(strings.NewReplacer("%", "").Replace(i2.MemLimit), 64)
			core = append(core, fmt.Sprintf(`Core:%02v Memory:%v(%0.2fGB) 并发:%v 类型:%v`,
				i2.CpuCoreLimit,
				i2.MemLimit,
				float64(c.Memory)*0.9*0.9*(memlimit/100),
				i2.ConcurrencyLimit,
				queryType,
			))
		}(i)
	}
	wg.Wait()

	srcData = data
	srcCore = core

	if v, ok := lastcache.Get(c.App + c.Username); ok {
		//go tools.WriteFile(v.(string), strings.Join(ds, "\n"))
		if htmltr != nil {
			go tools.WriteFile(v.(string), html_template(strings.Join(htmltr, "\n")))
		}
	}

	return
}

func regex(str string) (string, string) {
	// 正则表达式匹配 user 和 query_type
	re := regexp.MustCompile(`user=([a-zA-Z0-9_]+), query_type in \(([^)]+)\)`)
	matches := re.FindStringSubmatch(str)

	var user, queryType string
	if len(matches) > 2 {
		user = matches[1]
		queryType = matches[2]
	}
	return user, queryType
}
