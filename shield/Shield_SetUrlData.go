/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package shield
 *@file    Shield_SetUrlData
 *@date    2024/11/14 20:47
 */

package shield

import (
	"StarRocksQueris/util"
	"fmt"
)

func SetShieldUrlData(shield *util.ShieldUrls) error {
	r := util.Connect.Exec(fmt.Sprintf("insert into %s(shield_app,shield_name,shield_channel,shield_url) values('%s','%s','%d','%s')",
		shield.Tablename,
		shield.Data.ShieldApp,
		shield.Data.ShieldName,
		shield.Data.ShieldChannel,
		shield.Data.ShieldRequert,
	))
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return r.Error
	}
	return nil
}
