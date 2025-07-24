/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    init
 *@date    2025/1/10 16:22
 */

package short

import "StarRocksQueris/xid"

var shortdb string
var uid string

func init() {
	uid = xid.Xid(nil)
	logsrus()
}
