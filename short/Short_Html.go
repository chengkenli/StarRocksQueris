/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package short
 *@file    Short_Html
 *@date    2025/2/7 17:05
 */

package short

import (
	"StarRocksQueris/tools"
	"fmt"
)

func html_tr(tr []HtmlData, user string) []string {
	var trs []string
	for i, data := range tr {
		if i == 0 {
			msg := fmt.Sprintf(`
<tr>
	<td colspan="10">%s</td>
</tr>`, user)
			trs = append(trs, msg)
		}
		var color string
		ts := data.QueryTime / 1000
		if ts >= 60 && ts < 120 {
			color = `bgcolor="#FFFF6F"`
		} else if ts >= 120 && ts < 300 {
			color = `bgcolor="#FFB5B5"`
		} else if ts >= 300 {
			color = `bgcolor="#FF0000"`
		} else if data.QueryTime < 1000 {
			color = `bgcolor="#79FF79"`
		}

		msg := fmt.Sprintf(`
<tr %s>
	<td>%d</td>
	<td>%s</td>
	<td>%d</td>
	<td>%s</td>
	<td>%s</td>
	<td>%s</td>
	<td>%d</td>
	<td>%s</td>
	<td>%d</td>
	<td>%s</td>
</tr>`, color, data.Id, data.User, data.CpuCostNs,
			fmt.Sprintf("%d(%s)", data.MemCostBytes, ByteSizeToString(data.MemCostBytes)),
			fmt.Sprintf(`<a href="%s" target="_blank">%s(visible stmt)</a>`, data.Stmt, data.QueryId),
			fmt.Sprintf("%d(%s)", data.QueryTime, tools.GetHour(int(ts))), data.ReturnRows,
			fmt.Sprintf("%d(%s)", data.ScanBytes, ByteSizeToString(data.ScanBytes)), data.ScanRows, data.Timestamp)
		trs = append(trs, msg)
	}
	return trs
}

func html_template(value string) string {
	body := fmt.Sprintf(`
<pre>
	<table style="width:100%%;" cellpadding="2" cellspacing="0" border="1"
	bordercolor="#000000">
		<tbody>
			<tr>
				<td colspan="9">
					FIND ID
				</td>
				<td>
					%s
				</td>
			</tr>
			<tr>
				<td>
					ID
				</td>
				<td>
					USER
				</td>
				<td>
					CPUCOSTNS
				</td>
				<td>
					MEMCOSTBYTES
				</td>
				<td>
					QUERYID
				</td>
				<td>
					QUERYTIME
				</td>
				<td>
					RETURNROWS
				</td>
				<td>
					SCANBYTES
				</td>
				<td>
					SCANROWS
				</td>
				<td>
					TIMESTAMP
				</td>
			</tr>
            %s
		</tbody>
	</table>
</pre>`, fmt.Sprintf(`<a href="%s" target="_blank">%s</a>`, "", ""), value)
	return body
}
