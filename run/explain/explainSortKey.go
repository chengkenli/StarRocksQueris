/*
 *@author  chengkenli
 *@project setbuckets
 *@package service
 *@file    ScanSchemaSortKey
 *@date    2024/7/30 11:10
 */

package explain

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"gorm.io/gorm"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// ScanSchemaSortKey 排序键展示
func ScanSchemaSortKey(db *gorm.DB, olap []string) ([]*util.SchemaSortKey, error) {
	if olap == nil {
		return nil, nil
	}
	var m []*util.SchemaSortKey
	var wg sync.WaitGroup
	util.Loggrs.Info(strings.Join(olap, ","))
	for _, table := range olap {
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			scan, err := olapScan(db, table)
			if err != nil {
				return
			}
			m = append(m,
				&util.SchemaSortKey{
					Schema:  table,
					SortKey: scan,
				})
		}(table)
	}
	wg.Wait()
	return m, nil
}

func olapScan(db *gorm.DB, table string) (*util.SortKeys, error) {
	table = ExOlapOrView(db, table)
	//提取切割键
	var cqm []map[string]interface{}
	r := db.Raw(fmt.Sprintf("show create table %s", table)).Scan(&cqm)
	if r.Error != nil {
		return nil, r.Error
	}
	var createSQL string
	for _, m := range cqm {
		createSQL = fmt.Sprintf("%v", m["Create Table"])
	}
	var splikKey, sortKey []string
	for _, s2 := range strings.Split(createSQL, "\n") {
		if strings.Contains(s2, "DISTRIBUTED BY") || strings.Contains(s2, "KEY(`") {
			splikKey = append(splikKey, s2)
			re1 := regexp.MustCompile(`\((.*?)\)`)
			matches2 := re1.FindAllStringSubmatch(s2, -1)
			for _, i2 := range matches2 {
				s := strings.NewReplacer(" ", "", "`", "").Replace(i2[1])
				sortKey = append(sortKey, strings.Split(s, ",")...)
			}
		}
	}

	sortKey = tools.RemoveDuplicateStrings(sortKey)
	//分析切割键
	var cm []map[string]interface{}
	r = db.Raw("desc " + table).Scan(&cm)
	if r.Error != nil {
		return nil, r.Error
	}
	var fied []map[string]int64
	var FieldInt, FieldDecimal, FieldDate, FieldString []string

	ch := make(chan struct{}, 10)
	var wg sync.WaitGroup
	for _, m2 := range cm {
		wg.Add(1)
		m2 := m2
		go func() {
			defer func() {
				<-ch
				wg.Done()
			}()

			ch <- struct{}{}
			fied = append(fied, map[string]int64{m2["Field"].(string): scanCountDistinct(db, m2["Field"].(string), table)})
			if strings.Contains(strings.ToLower(m2["Type"].(string)), "int") {
				FieldInt = append(FieldInt, m2["Field"].(string))
			}
			if strings.Contains(strings.ToLower(m2["Type"].(string)), "decimal") {
				FieldDecimal = append(FieldDecimal, m2["Field"].(string))
			}
			if strings.Contains(strings.ToLower(m2["Type"].(string)), "date") {
				FieldDate = append(FieldDate, m2["Field"].(string))
			}
			if strings.Contains(strings.ToLower(m2["Type"].(string)), "varchar") {
				FieldString = append(FieldString, m2["Field"].(string))
			}
		}()
	}
	wg.Wait()

	//test
	var intSort, decimalSort, dateSort, stringSort []map[string]int64
	for _, s2 := range FieldInt {
		adss, _ := tools.FindKeyRank(fied, s2)
		intSort = append(intSort, map[string]int64{s2: fied[adss][s2]})
	}
	for _, s2 := range FieldDecimal {
		adss, _ := tools.FindKeyRank(fied, s2)
		decimalSort = append(decimalSort, map[string]int64{s2: fied[adss][s2]})
	}
	for _, s2 := range FieldDate {
		adss, _ := tools.FindKeyRank(fied, s2)
		dateSort = append(dateSort, map[string]int64{s2: fied[adss][s2]})
	}
	for _, s2 := range FieldString {
		adss, _ := tools.FindKeyRank(fied, s2)
		stringSort = append(stringSort, map[string]int64{s2: fied[adss][s2]})
	}
	// [int]定义一个比较函数，用于sort包的比较接口
	sort.Slice(intSort, func(i, j int) bool {
		// 计算每个map的int64值总和
		sumI := tools.SumMapValues(intSort[i])
		sumJ := tools.SumMapValues(intSort[j])
		// 按照总和进行降序排序
		return sumI > sumJ
	})
	// [decimal]定义一个比较函数，用于sort包的比较接口
	sort.Slice(decimalSort, func(i, j int) bool {
		// 计算每个map的int64值总和
		sumI := tools.SumMapValues(decimalSort[i])
		sumJ := tools.SumMapValues(decimalSort[j])
		// 按照总和进行降序排序
		return sumI > sumJ
	})
	// [date]定义一个比较函数，用于sort包的比较接口
	sort.Slice(dateSort, func(i, j int) bool {
		// 计算每个map的int64值总和
		sumI := tools.SumMapValues(dateSort[i])
		sumJ := tools.SumMapValues(dateSort[j])
		// 按照总和进行降序排序
		return sumI > sumJ
	})
	// [string]定义一个比较函数，用于sort包的比较接口
	sort.Slice(stringSort, func(i, j int) bool {
		// 计算每个map的int64值总和
		sumI := tools.SumMapValues(stringSort[i])
		sumJ := tools.SumMapValues(stringSort[j])
		// 按照总和进行降序排序
		return sumI > sumJ
	})

	var split, intarr, decarr, datearr, strarr []string
	//数值
	if len(intSort) >= 1 {
		for key := range intSort[0] {
			split = append(split, fmt.Sprintf("%s(int)", key))
		}
		for _, v := range intSort {
		labeli:
			for s1, i := range v {
				for _, s2 := range sortKey {
					if s1 == s2 {
						intarr = append(intarr, fmt.Sprintf("[%s]:(%d)", s1, i))
						break labeli
					}
				}
				intarr = append(intarr, fmt.Sprintf("[%s]:(%d)", s1, i))
			}
		}
	}
	//小数点
	if len(decimalSort) >= 1 {
		for key := range decimalSort[0] {
			split = append(split, fmt.Sprintf("%s(decimal)", key))
		}
		for _, v := range decimalSort {
		labelds:
			for s1, i := range v {
				for _, s2 := range sortKey {
					if s1 == s2 {
						decarr = append(decarr, fmt.Sprintf("[%s]:(%d)", s1, i))
						break labelds
					}
				}
				decarr = append(decarr, fmt.Sprintf("[%s]:(%d)", s1, i))
			}
		}
	}
	//日期
	if len(dateSort) >= 1 {
		for key := range dateSort[0] {
			split = append(split, fmt.Sprintf("%s(date)", key))
		}
		for _, v := range dateSort {
		labeld:
			for s1, i := range v {
				for _, s2 := range sortKey {
					if s1 == s2 {
						datearr = append(datearr, fmt.Sprintf("[%s]:(%d)", s1, i))
						break labeld
					}
				}
				datearr = append(datearr, fmt.Sprintf("[%s]:(%d)", s1, i))
			}
		}
	}
	//字符串
	if len(stringSort) >= 1 {
		for key := range stringSort[0] {
			split = append(split, fmt.Sprintf("%s(string)", key))
		}
		for _, v := range stringSort {
		labels:
			for s1, i := range v {
				for _, s2 := range sortKey {
					if s1 == s2 {
						strarr = append(strarr, fmt.Sprintf("[%s]:(%d)", s1, i))
						break labels
					}
				}
				strarr = append(strarr, fmt.Sprintf("[%s]:(%d)", s1, i))
			}
		}

		// 处理最佳排序键的max and min
		ch2 := make(chan struct{}, 10)
		var splitKeys []string
		var wgs sync.WaitGroup
		for _, key := range split {
			wgs.Add(1)
			go func(key string) {
				defer func() {
					<-ch2
					wgs.Done()
				}()

				ch2 <- struct{}{}
				keyv := strings.Split(key, "(")[0]
				sql := fmt.Sprintf("select max(%s) as max,min(%s) as min from (select count(*),%s from (select DISTINCT %s from (select %s from %s limit 1000) a ) b group by %s) c",
					keyv, keyv, keyv, keyv, keyv,
					table,
					keyv,
				)
				var m map[string]interface{}
				r := db.Raw(sql).Scan(&m)
				if r.Error != nil {
					util.Loggrs.Error(r.Error.Error())
				}
				splitKeys = append(splitKeys, fmt.Sprintf("%s(max:%v,min:%v)", key, m["max"], m["min"]))
			}(key)
		}
		wgs.Wait()

		return &util.SortKeys{
			SplikKey:  splikKey,
			SortKey:   sortKey,
			SplitKeys: splitKeys,
			IntArr:    intarr,
			DecArr:    decarr,
			DateArr:   datearr,
			StrArr:    strarr,
		}, nil
	}
	return &util.SortKeys{
		SplikKey:  splikKey,
		SortKey:   sortKey,
		SplitKeys: nil,
		IntArr:    intarr,
		DecArr:    decarr,
		DateArr:   datearr,
		StrArr:    strarr,
	}, nil
}

// ScanCountDistinct 统计每个字段1000行内重复次数
func scanCountDistinct(db *gorm.DB, column, table string) int64 {
	var m []map[string]interface{}
	sql := fmt.Sprintf(`select count(*),%s from (select DISTINCT %s from (select %s from %s limit 1000) a ) b group by %s`, column, column, column, table, column)
	r := db.Raw(sql).Scan(&m)
	if r.Error != nil {
		return 0
	}
	return r.RowsAffected
}
