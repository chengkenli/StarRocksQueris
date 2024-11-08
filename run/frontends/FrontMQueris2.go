/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendHandleOnBigQuery
 *@date    2024/9/3 17:03
 */

package fronends

import (
	"StarRocksQueris/util"
	"gorm.io/gorm"
)

func QuerisA(db *gorm.DB, app, fe string, m2 *util.Querisign) util.SessionBigQuery {
	stmt := UriCurrentQueriesStmt(app, fe, m2.QueryId)
	host := UriCurrentQueriesHosts(app, fe, m2.QueryId)
	return util.SessionBigQuery{
		Db:            db,
		StartTime:     m2.StartTime,
		QueryId:       m2.QueryId,
		ConnectionId:  m2.ConnectionId,
		Database:      m2.Database,
		User:          m2.User,
		ScanBytes:     m2.ScanBytes,
		ScanRows:      m2.ScanRows,
		MemoryUsage:   m2.MemoryUsage,
		DiskSpillSize: m2.DiskSpillSize,
		CPUTime:       m2.DiskSpillSize,
		ExecTime:      m2.ExecTime,
		Nodes:         host,
		Stmt:          stmt,
	}
}
