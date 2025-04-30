/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package xid
 *@file    xid
 *@date    2025/4/30 14:27
 */

package xid

import (
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"regexp"
	"time"
)

type Uid struct {
	App  string
	Fe   string
	Mode string
	Id   string
}

func Xid(u *Uid) string {
	if u == nil {
		return uuid.New().String() + " "
	}
	uxd := fmt.Sprintf("%s_%s_%s_%s", u.App, u.Fe, u.Mode, u.Id)
	return strscore(replacescores(uxd)) + " "
}
func replacescores(s string) string {
	re := regexp.MustCompile(`_{2,}`)
	return re.ReplaceAllString(s, "_")
}
func strscore(s string) string {
	length := 36
	if len(s) == length {
		return s
	}
	if len(s) > length {
		return s[0:36]
	}
	paddingLength := length - len(s)
	return s + generateRandomString(paddingLength)
}

// generateRandomString creates a random string of the given length.
func generateRandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
