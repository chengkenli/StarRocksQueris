/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontend
 *@date    2024/8/8 14:15
 */

package fronends

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"
)

func SessionSchemaRegexp(input string) ([]string, error) {
	database := []string{
		"adhoc", "ads", "ads_dev", "ads_dev_secure", "ads_rt", "ads_rt_dev", "ads_rt_dev_secure", "ads_rt_secure", "ads_secure",
		"algo", "algo_dev", "ap_secure", "audit", "bi_item", "bi_realty", "bi_realty_secure", "bi_sams_secure", "bi_scm", "bi_sc_secure",
		"cdp", "cdp_api", "cloud_fcst_dm", "cn_backup_secure", "cn_chilled_data", "cn_core_dim_vm", "cn_di_data", "cn_ec_bi_secure",
		"cn_ec_wmdj_user_action", "cn_mdse_dm_dl_tables", "cn_po_home_system", "cn_po_home_system_dev", "cn_pricing_dl_tables",
		"cn_sams_dl_secure", "cn_wc_highsecure", "cn_wc_mb_secure", "cn_wc_mb_vm", "cn_wc_repl_vm", "cn_wc_vm", "cn_wid_dl_secure",
		"cn_wm_mb_secure", "cn_wm_mb_vm", "cn_wm_repl_vm", "cn_wm_vm", "data_test", "demo", "dim", "dim_dev", "dim_dev_secure", "dim_rt",
		"dim_rt_dev", "dim_rt_dev_secure", "dim_rt_secure", "dim_secure", "dm", "dm_dev", "dm_dev_secure", "dm_secure", "dw", "dwd", "dwd_dev",
		"dwd_dev_secure", "dw_dev", "dw_dev_secure", "dwd_rt", "dwd_rt_dev", "dwd_rt_dev_secure", "dwd_rt_secure", "dwd_secure", "dw_rt",
		"dws", "dws_dev", "dws_dev_secure", "dw_secure", "dws_rt", "dws_rt_dev", "dws_rt_dev_secure", "dws_rt_secure", "dws_secure",
		"euclid_scn_forecast_prod", "finance_kettle", "fin_sox", "fin_sox_dev", "flash_report", "flash_report_dev", "flash_report_sit",
		"hyper_bi_secure", "hyper_ec_secure", "hyper_mdse_dm_secure", "information_schema", "ma_test", "mbrship_secure", "mcfc_report",
		"mcfc_report_dev", "o2o_datacubes_secure", "ods", "ods_app_dev", "ods_app_dev_secure", "ods_app_test", "ods_app_test_secure",
		"ods_archive", "ods_dev", "ods_dev_secure", "ods_gray", "ods_migration_td_gray", "ods_rt", "ods_rt_dev", "ods_rt_dev_secure",
		"ods_rt_secure", "ods_secure", "ods_secure_rt", "ods_sox", "ods_sox_app_dev", "ods_sox_app_test", "ods_sox_dev", "ods_sox_test",
		"ods_test", "ods_test_secure", "ops", "pro_dgtmkt_data", "pro_scct_dev", "sams_finance", "scct_inv_monitor", "scct_logis", "scct_logis_dev",
		"scm_dcqe_secure", "scm_network_secure", "scm_secure", "scm_uihealth", "starrocks_monitor", "_statistics_", "supply_kettle", "svccn_logis",
		"svccn_logis_query", "svcdordgtmkt", "sys", "wm_ad_hoc", "wm_cn_util", "wm_common_vm", "ww_core_dim_vm"}

	if strings.Contains(input, "hadoop") && !strings.Contains(strings.ToLower(input), "outfile") {
		return nil, nil
	}
	var schema []string
	/*schema.table*/
	re := regexp.MustCompile(`([a-zA-Z][^\s=,'.]+)\.([^\s=,'.]+)`)
	var result []string
	for _, s := range re.FindAllString(input, -1) {
		b := regexp.MustCompile(`[\\/\(\),:|+><~!@#%^&*='";?-]`).FindString(s) != ""
		if !b {
			data := strings.Split(s, ".")
			for _, s2 := range database {
				if data[0] == s2 {
					result = append(result, s)
				}
			}
		}
	}
	/*catalog.schema.table*/
	re2 := regexp.MustCompile(`([a-zA-Z][^\s=,'.]+)\.([^\s=,'.]+)\.([^\s=,'.]+)`)
	var result2 []string
	for _, s := range re2.FindAllString(input, -1) {
		b := regexp.MustCompile(`[\\/\(\),:|+><~!@#%^&*='";?-]`).FindString(s) != ""
		if !b {
			data := strings.Split(s, ".")
			for _, s2 := range database {
				if data[1] == s2 {
					result2 = append(result2, s)
				}
			}
		}
	}
	schema = append(schema, result...)
	schema = append(schema, result2...)

	return tools.RemoveDuplicateStrings(schema), nil
}

// SessionSchemaRegexpOwner 解析表中的主题域与owner、邮件等
func SessionSchemaRegexpOwner(uid int, schema []string) *util.EmailMain {
	var email, domain []string
	for _, s := range schema {
		/*匹配ods*/
		/*匹配catalog*/
		if strings.Contains(s, "hive.") || strings.Contains(s, "iceberg.") || strings.Contains(s, "default_catalog.") || strings.Contains(s, "sradhoc.") || strings.Contains(s, "srapp.") {
			data := strings.Split(s, ".")
			if len(data) < 3 {
				continue
			}
			text := strings.Split(data[2], "_")
			if len(text) < 2 {
				continue
			}
			zty := strings.Split(data[2], "_")[1]

			re := regexp.MustCompile("[\u4e00-\u9fff]+")
			if !re.MatchString(zty) {
				domain = append(domain, zty)
			}

			for _, d := range util.Domain {
				if d[zty] == "" {
					continue
				}
				for _, u := range strings.Split(d[zty], ",") {
					if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
						email = append(email, fmt.Sprintf("%s%s", u, util.ConnectNorm.SlowQueryEmailSuffix))
					} else {
						email = append(email, u)
					}
				}
			}
			continue
		}

		data := strings.Split(s, ".")
		if len(data) < 2 {
			continue
		}
		text := strings.Split(data[1], "_")
		if len(text) < 2 {
			continue
		}
		zty := strings.Split(data[1], "_")[1]
		re := regexp.MustCompile("[\u4e00-\u9fff]+")
		if !re.MatchString(zty) {
			domain = append(domain, zty)
		}
		for _, d := range util.Domain {
			if d[zty] == "" {
				continue
			}
			for _, u := range strings.Split(d[zty], ",") {
				if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
					email = append(email, fmt.Sprintf("%s%s", u, util.ConnectNorm.SlowQueryEmailSuffix))
				} else {
					email = append(email, u)
				}
			}
		}
	}

	var cc []string
	if uid == 1 {
		if email != nil {
			if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
				cc = SchemaDomainGroup(strings.ReplaceAll(email[0], util.ConnectNorm.SlowQueryEmailSuffix, ""))
			}
		}
	}

	em := &util.EmailMain{
		Domain:  tools.RemoveDuplicateStrings(domain),
		EmailTo: tools.RemoveDuplicateStrings(email),
		EmailCc: cc,
	}

	return em
}

// SchemaDomainGroup 获取leader下的所有下属
func SchemaDomainGroup(leader string) []string {
	if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
		if !strings.Contains(util.ConnectNorm.SlowQueryEmailSuffix, "wal-mart.com") {
			return nil
		}
	}

	/*该模式属于企业专用，屏蔽*/
	type group struct {
		ID string `json:"id"`
	}
	var g group
	err := json.Unmarshal(nil, &g)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	if len(g.ID) == 0 {
		return nil
	}
	var email []string
	for _, s := range strings.Split(g.ID, ",") {
		if len(util.ConnectNorm.SlowQueryEmailSuffix) != 0 {
			email = append(email, s+util.ConnectNorm.SlowQueryEmailSuffix)
		}
	}
	return email
}

func Post(method, url string, body io.Reader) []byte {
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	request.Header.Set("Content-Type", "application/json;charset=utf-8")
	client := &http.Client{Timeout: time.Second * 30}
	respone, err := client.Do(request)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	defer respone.Body.Close()
	b, err := ioutil.ReadAll(respone.Body)
	if err != nil {
		util.Loggrs.Error(err.Error())
		return nil
	}
	return b
}
