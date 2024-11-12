/*
 *@author  chengkenli
 *@project StarRocksFeMonitor
 *@package util
 *@file    onestop
 *@date    2024/7/4 15:45
 */

package util

import (
    "bytes"
    "encoding/json"
    "io/ioutil"
    "net/http"
    "strings"
)

/* OneStopZtyo 主题域*/
func OneStopZtyo() {
    type fileter struct {
        ControlId  string `json:"controlId"`
        DataType   int    `json:"dataType"`
        SpliceType int    `json:"spliceType"`
        FilterType int    `json:"filterType"`
        Value      string `json:"value"`
    }
    type test1 struct {
        AppKey      string    `json:"appKey"`
        Sign        string    `json:"sign"`
        WorksheetId string    `json:"worksheetId"`
        ViewId      string    `json:"viewId"`
        PageSize    int       `json:"pageSize"`
        PageIndex   int       `json:"pageIndex"`
        SortId      string    `json:"sortId"`
        IsAsc       bool      `json:"isAsc"`
        Filters     []fileter `json:"filters"`
        //Filters     struct{}
        NotGetTotal      bool `json:"notGetTotal"`
        UseControlId     bool `json:"useControlId"`
        GetSystemControl bool `json:"getSystemControl"`
    }
    type data struct {
        Data struct {
            Rows []struct {
                ID    string `json:"_id"`
                Rowid string `json:"rowid"`
                Ctime string `json:"ctime"`
                Caid  struct {
                    AccountID string `json:"accountId"`
                    Fullname  string `json:"fullname"`
                    Avatar    string `json:"avatar"`
                    IsPortal  bool   `json:"isPortal"`
                    Status    int    `json:"status"`
                } `json:"caid"`
                Uaid struct {
                    AccountID string `json:"accountId"`
                    Fullname  string `json:"fullname"`
                    Avatar    string `json:"avatar"`
                    IsPortal  bool   `json:"isPortal"`
                    Status    int    `json:"status"`
                } `json:"uaid"`
                Ownerid struct {
                    AccountID string `json:"accountId"`
                    Fullname  string `json:"fullname"`
                    Avatar    string `json:"avatar"`
                    IsPortal  bool   `json:"isPortal"`
                    Status    int    `json:"status"`
                } `json:"ownerid"`
                Utime   string `json:"utime"`
                Ztmc    string `json:"ztmc"`
                Ztowner []struct {
                    AccountID string `json:"accountId"`
                    Fullname  string `json:"fullname"`
                    Avatar    string `json:"avatar"`
                    Status    int    `json:"status"`
                } `json:"ztowner"`
                Ztymc              string `json:"ztymc"`
                Ownerid1           string `json:"ownerid1"`
                Autoid             int    `json:"autoid"`
                Ztjc               string `json:"ztjc"`
                Allowdelete        bool   `json:"allowdelete"`
                Controlpermissions string `json:"controlpermissions"`
            } `json:"rows"`
            Total int `json:"total"`
        } `json:"data"`
        Success   bool `json:"success"`
        ErrorCode int  `json:"error_code"`
    }

    test := test1{}

    marshal, errM := json.Marshal(test)
    if errM != nil {
        Loggrs.Error(errM.Error())
    }
    request1, err1 := http.NewRequest("POST", "https://xxxxxx/api/v2/open/worksheet/getFilterRows", bytes.NewBuffer(marshal))
    if err1 != nil {
        Loggrs.Error(err1.Error())
    }
    request1.Header.Set("Content-Type", "application/json")
    do1, _ := (&http.Client{}).Do(request1)
    defer do1.Body.Close()
    all1, _ := ioutil.ReadAll(do1.Body)

    var d data
    replaceAll := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(string(all1), "\\", ""), "\"[", "["), "]\"", "]")
    json.Unmarshal([]byte(replaceAll), &d)
    for _, r := range d.Data.Rows {
        Domain = append(Domain, map[string]string{r.Ztmc: r.Ownerid1})
    }
}
