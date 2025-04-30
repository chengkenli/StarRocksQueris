/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package etrics
 *@file    init
 *@date    2025/4/30 16:33
 */

package etrics

import "StarRocksQueris/xid"

var uid string

func init() {
	uid = xid.Xid(nil)
}
