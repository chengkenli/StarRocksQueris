/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package roboot
 *@file    SendEmSessionQuery
 *@date    2024/8/8 14:29
 */

package robot

import (
	"StarRocksQueris/tools"
	"StarRocksQueris/util"
	"fmt"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

// SendEmQueris 发送邮件告警
func SendEmQueris(i *WarnQuerisEmail) {
	if util.ConnectNorm["slow_query_email_host"] == "" {
		return
	}
	if util.ConnectNorm["slow_query_email_from"] == "" {
		return
	}
	if util.ConnectNorm["slow_query_email_to"] == "" {
		return
	}

	edtime, _ := strconv.Atoi(i.Queris.Item.Time)

	// 如果是行为2（仅提醒的公告，不发邮件）
	//if i.Queris.Action == 2 {
	//	return
	//}
	//if i.Queris.Action == 0 {
	//	return
	//}

	var anay1, anay2 []string
	var sessionquery string
	if len(i.Queris.Item.Info) >= 100 {
		sessionquery = i.Queris.Item.Info[0:80]
	} else {
		sessionquery = i.Queris.Item.Info
	}

	var version string
	if util.ConnectNorm["slow_query_version"] != nil {
		version = util.ConnectNorm["slow_query_version"].(string)
	}

	hmsg := fmt.Sprintf("【%s】%s", i.Queris.Sign, version)
	var headmsg string
	if edtime >= int(util.ConnectNorm["slow_query_ktime"].(int32)) {
		headmsg = span("目前已被查杀！")
	}
	switch i.Queris.Action {
	case 0, 1, 3, 4, 5, 6:
		headmsg = span("目前已被查杀！")
	}
	// 其他
	anay2 = append(anay2, li(fmt.Sprintf("请尽快处理该问题，以免影响业务正常运行。")))
	if util.ConnectNorm["slow_query_email_reference_material"] != nil {
		material := strings.Split(util.ConnectNorm["slow_query_email_reference_material"].(string), ",")
		for i, m := range material {
			anay2 = append(anay2, li(fmt.Sprintf(`#%d %s`, i, m)))
		}
	}
	// 拦截原因
	if i.Queris.Opinion != "" {
		anay1 = append(anay1, li(colred(i.Queris.Opinion)))
	}
	if i.Queris.Action == 6 {
		li(fmt.Sprintf(`catalog - 从catalog通过insert方式写入的数据量过大，目前已经被拦截！请使用BROKER LOAD的方式进行导入！`))
	}
	if len(i.Avgs) != 0 {
		anay1 = append(anay1, li(fmt.Sprintf("请检查目前慢查询所带的系统参数是否合理，目前%s参数过高存在节点过载风险已被拦截，请尽快调整。", u1(strings.Join(i.Avgs, ",")))))
		anay1 = append(anay1, li(fmt.Sprintf("参数参考值：%s，%s(%s)", u1("并发数不允许超出10"), u1("内存不允许超出100GB"), strong("107374182400"))))
	}
	if len(i.Queris.Iceberg) > 1 {
		anay1 = append(anay1, li(colred(i.Queris.Iceberg)))
	}
	if i.Queris.Queris != nil {
		anay1 = append(anay1, li(fmt.Sprintf("目前查询扫描过程中消耗%s内存，计算内存使用%s。", strong(i.Queris.Queris.ScanBytes), strong(i.Queris.Queris.MemoryUsage))))
		anay1 = append(anay1, li(fmt.Sprintf("目前检测到%s并且扫描数据量高达%s，请尽快调整。", strong("全表扫描"), strong(i.Queris.Queris.ScanRows))))
	} else {
		if i.Queris.Olapscan != nil {
			if i.Queris.Olapscan.OlapScan {
				anay1 = append(anay1, li(fmt.Sprintf("您的慢查询属于%s，请调整数据扫描范围，进行分区裁剪，降低扫描数据量。", strong("全表扫描"))))
			}
		}
	}
	// 慢查询原因分析
	if edtime >= int(util.ConnectNorm["slow_query_time"].(int32)) {
		anay1 = append(anay1, li(fmt.Sprintf("请检查目前慢查询消耗时间是否一直稳定在%s左右，耗时较长。", strong(tools.GetHour(edtime)))))
	}
	if len(i.Queris.Schema) > 1 {
		anay1 = append(anay1, li(fmt.Sprintf("请检查目前关联的库表是否过多，目前关联了%s张表。", strong(fmt.Sprintf("%d", len(i.Queris.Schema))))))
	}
	if !i.Queris.Normal {
		anay1 = append(anay1, li(fmt.Sprintf("目前关联的表中存在分桶%s，请按照标准进行调整内表分桶。", strong("倾斜"))))
	}
	if len(i.Queris.Queryid) >= 2 {
		anay1 = append(anay1, li(fmt.Sprintf("目前该类语句已经发起了%s次，请尽快优化查询SQL，提高查询性能。", strong(fmt.Sprintf("%d", len(i.Queris.Queryid))))))
	}
	anay1 = append(anay1, li(fmt.Sprintf("请检查慢查询涉及的数据表，确认是否存在不合理的分区策略。")))
	anay1 = append(anay1, li(fmt.Sprintf("请检查慢查询语句，确认是否存在优化空间，如索引使用、查询条件调整等。")))

	msg := fmt.Sprintf(`
<hr />
主题：StarRocks慢查询告警：%s<br />
<br />
尊敬的%s：<br />
<br />
您好！慢查询监控系统发现，您有一笔StarRocks查询请求执行缓慢 %s，以下是详细信息：<br />
<br />
<p>
	查询ID：%s， 开始时间：%s， 结束时间：%s， 执行时长：%s， 集群/数据库：%s， 用户：%s
</p>
<p>
	告警的慢查询语句是：%s...<a href="%s" target="_blank">展开更多</a>
</p>
<br />
慢查询原因分析：<br />
<ol>
	%s
</ol>
<br />
其他：<br />
<ol>
	%s
</ol>
<br />
<br />
祝工作愉快！<br />
<p>
	此电子邮件由系统自动发送，请勿直接回复此电子邮件。
</p>
<p>
	<hr />
</p>
<br />
<br />`,
		strong(i.Queris.Item.Id),
		strong(i.Queris.Item.User),
		headmsg,
		strong(i.Queris.Item.Id),
		strong(time.Now().Add(-time.Second*time.Duration(edtime)).Format("2006-01-02 15:04:05")),
		strong("active"),
		strong(tools.GetHour(edtime)),
		em(tools.HostApp(i.Queris.App)),
		strong(i.Queris.Item.User),
		u1(sessionquery),
		fmt.Sprintf("http://%s:9977/log%s", util.H.Ip, i.Queris.Logfile),
		strings.Join(anay1, " "),
		strings.Join(anay2, " "),
	)

	var tc, to, bc string
	var cc []string
	if util.ConnectNorm["slow_query_email_suffix"] != nil {
		if strings.Contains(util.ConnectNorm["slow_query_email_suffix"].(string), "wal-mart.com") {
			tc, to, cc = MetaData(i.Queris.App, i.Queris.Item.User)
			util.Loggrs.Info(tc)
		} else {
			to = util.ConnectNorm["slow_query_email_to"].(string)
			if util.ConnectNorm["slow_query_email_cc"] != "" {
				cc = append(cc, util.ConnectNorm["slow_query_email_cc"].(string))
			}
		}
	}
	if util.ConnectNorm["slow_query_email_bc"] != nil {
		bc = util.ConnectNorm["slow_query_email_bc"].(string)
	}

	util.Loggrs.Info(fmt.Sprintf("id:%s send to:%s cc:%v bc:%v", i.Queris.Item.Id, to, cc, bc))

	if util.P.Check {
		return
	}
	SendEmail(&util.Emailinfo{
		Subject: fmt.Sprintf("%s 主题：StarRocks慢查询告警，集群：%s，用户：%s", hmsg, i.Queris.App, i.Queris.Item.User),
		To:      to,
		From:    util.ConnectNorm["slow_query_email_from"].(string),
		Cc:      cc,
		Bc:      bc,
		Attach:  "",
		Emsg:    msg,
	})

	cid := fmt.Sprintf("email_%s", i.Queris.Item.Id)
	util.Loggrs.Info(fmt.Sprintf("***************邮件:%s加入缓存", cid))
	i.Queris.EmCache.Set(cid, i.Queris.Item.Id, cache.DefaultExpiration)
}

// 标签 - 粗体
func strong(str string) string {
	return fmt.Sprintf(`<strong>%s</strong>`, str)
}

// 标签 - 斜体+下划线
func em(str string) string {
	return fmt.Sprintf(`<u><em>%s</u></em>`, str)
}

// 标签 - 斜体
func u1(str string) string {
	return fmt.Sprintf(`<u>%s</u>`, str)
}

// 标签 - 编号
func li(str string) string {
	return fmt.Sprintf(`<li>%s</li>`, str)
}

// 标签 - 前景黄色，背景红色
func span(str string) string {
	return fmt.Sprintf(`<span style="background-color:#E53333;color:#FFE500;">%s</span>`, str)
}

// 标签 - 红色颜色字体
func colred(str string) string {
	return fmt.Sprintf(`<span style="color:#E53333;">%s</span>`, str)
}
