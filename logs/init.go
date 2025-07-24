/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package logs
 *@file    init
 *@date    2025/4/30 12:19
 */

package logs

import "StarRocksQueris/xid"

const keyRequestId = "requestId"

var uid string

func init() {
	uid = xid.Xid(nil)
}
