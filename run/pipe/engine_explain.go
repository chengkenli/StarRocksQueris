/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    Run_Emo_Explain
 *@date    2025/1/23 9:28
 */

package pipe

import (
	"StarRocksQueris/run/explain"
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"fmt"
	"gorm.io/gorm"
	"time"
)

// comp explain
func emoExplain(db *gorm.DB, app string, item *util.Process2) *util.ResultData {

	uid := xid.Xid(&xid.Uid{
		App:  app,
		Fe:   "",
		Mode: "Explain",
		Id:   item.Id,
	})
	nt := time.Now()
	// ------------------------------------------
	// 【解析语句，提取表名】
	schema, err := SessionSchemaRegexp(item.Info)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("查询表名提取 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【获取内表副本分布情况，排序键分析】
	nt = time.Now()
	sortReport, olap, err := explain.ReplicaDistribution(db, schema)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("副本分布分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【实用分区与空分区的获取】
	nt = time.Now()
	rangerMap := explain.IsPartitionMap(db, olap)
	util.Loggrs.Info(uid, fmt.Sprintf("分区数量统计 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【解析执行计划】
	nt = time.Now()
	olapscan, exfile, err := explain.ExplainQuery(db, item, rangerMap)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("查询计划分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【排序键分析】
	nt = time.Now()
	sortkey, err := explain.ScanSchemaSortKey(db, olap)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("前排序键分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【分桶倾斜分析】
	nt = time.Now()
	buckets, normal, err := explain.GetBuckets(app, olap)
	if err != nil {
		util.Loggrs.Error(uid, err.Error())
	}
	util.Loggrs.Info(uid, fmt.Sprintf("分桶倾斜分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	// ------------------------------------------
	// 【分析查询语句与已经入库的语句相似百分比】
	nt = time.Now()
	util.Loggrs.Info(uid, fmt.Sprintf("余弦相似分析 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))
	queryid := TFIDF(item.Info)
	// ------------------------------------------
	// 【裁判内表视图】
	nt = time.Now()
	var tbs []string
	for _, tbname := range schema {
		table := explain.ExOlapOrView(db, tbname)
		if tbname == table {
			tbs = append(tbs, tbname)
		} else {
			tbs = append(tbs, fmt.Sprintf("%s(%s)", tbname, table))
		}
	}
	util.Loggrs.Info(uid, fmt.Sprintf("裁判内表视图 %s %s %v", app, item.Id, time.Now().Sub(nt).String()))

	return &util.ResultData{
		SortReport:   sortReport,
		OlapSchema:   schema,
		OlapTables:   olap,
		OlapScan:     olapscan,
		ExplainFile:  exfile,
		SortKeys:     sortkey,
		BucketResult: buckets,
		BucketType:   normal,
		QueryIds:     queryid,
		OlapView:     tbs,
	}
}
