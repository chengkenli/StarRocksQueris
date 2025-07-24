/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package shield
 *@file    Shield_GetData
 *@date    2024/11/14 20:32
 */

package shield

import (
	"StarRocksQueris/util"
	"fmt"
)

// GetShieldData 从交互表中获取数据
func GetShieldData(shield *util.Shields) (int, error) {
	var m map[string]interface{}
	r := util.Connect.Raw(fmt.Sprintf("select status from %s where shield_app='%s' and shield_name='%s' and shield_channel='%d'",
		shield.Tablename,
		shield.Data.ShieldApp,
		shield.Data.ShieldName,
		shield.Data.ShieldChannel,
	)).Scan(&m)
	if r.Error != nil {
		util.Loggrs.Error(r.Error.Error())
		return -1, r.Error
	}
	return m["status"].(int), nil
}
