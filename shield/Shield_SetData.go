/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package shield
 *@file    Shield_SetData
 *@date    2024/11/14 20:33
 */

package shield

import (
	"StarRocksQueris/util"
	"fmt"
)

// SetShieldData 往交互表中插入数据
func SetShieldData(shield *util.Shields) error {
	r := util.Connect.Exec(fmt.Sprintf("insert into %s(shield_app,shield_name,shield_channel) values('%s','%s','%d')",
		shield.Tablename,
		shield.Data.ShieldApp,
		shield.Data.ShieldName,
		shield.Data.ShieldChannel,
	))
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return r.Error
	}
	return nil
}
