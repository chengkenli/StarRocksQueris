package robot

import (
	"StarRocksQueris/util"
	"encoding/json"
	"fmt"
	"github.com/jordan-wright/email"
	"strings"
	"time"
)

func SendEmail(m *util.Emailinfo) {
	if util.ConnectNorm["slow_query_email_host"] == nil {
		return
	}

	if strings.Contains(m.To, "@") && m.Subject != "" && strings.Contains(m.From, "@") {
		emailBody := email.Email{}
		var (
			carbonCopy, blindCarbonCopy []string
		)
		addressee := strings.Split(m.To, ",")
		if strings.Contains(strings.Join(m.Cc, ","), "@") {
			carbonCopy = m.Cc
		}
		if strings.Contains(m.Bc, "@") {
			blindCarbonCopy = strings.Split(m.Bc, ",")
		}
		if m.Attach != "" {
			for _, f := range strings.Split(m.Attach, ",") {
				_, err := emailBody.AttachFile(f)
				if err != nil {
					util.Loggrs.Warn("Can not add file: " + f)
				}
			}
		}
		emailBody.Subject = m.Subject
		emailBody.Cc = carbonCopy
		emailBody.Bcc = blindCarbonCopy
		emailBody.To = addressee
		emailBody.From = m.From
		if true {
			emailBody.HTML = []byte(m.Emsg)
		} else {
			emailBody.Text = []byte(m.Emsg)
		}
		body, _ := json.MarshalIndent(emailBody, "", " ")

		// 新的机制
		p, err := email.NewPool(util.ConnectNorm["slow_query_email_host"].(string), 3, nil)
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		var e email.Email
		err = json.Unmarshal(body, &e)
		if err != nil {
			util.Loggrs.Error(err.Error())
			return
		}
		for i := 0; i < 3; i++ {
			err = p.Send(&e, 600*time.Second)
			if err != nil {
				if i != 2 {
					util.Loggrs.Warn(fmt.Sprintf("#%d send email(%s) to %s failed. err:%s", i+1, e.Subject, e.To, err.Error()))
					time.Sleep(3 * time.Second)
					continue
				}
				util.Loggrs.Error(fmt.Sprintf("send email(%s) to %s failed. err:%s", e.Subject, e.To, err.Error()))
			} else {
				util.Loggrs.Info(fmt.Sprintf("send email(%s) to %s success.", e.Subject, e.To))
				break
			}
		}

	}
}
