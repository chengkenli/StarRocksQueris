/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    emo_context
 *@date    2024/11/12 22:37
 */

package pipe

import (
	"StarRocksQueris/util"
	"StarRocksQueris/xid"
	"golang.org/x/net/context"
	"time"
)

// EmoContext 设置超时机制，如果100秒内没跑完，那么就中止运行，不要影响到下一批调度
func EmoContext() {
	uid := xid.Xid(nil)
	util.Loggrs.Info(uid, "Job 进入context withtimeout机制. ", util.ConnectNorm.SlowQueryTime)
	// 创建一个context，设置100秒的超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer func() {
		cancel()
		util.Loggrs.Info(uid, "Job 释放资源.")
	}() // 取消上下文，以释放资源
	var done = make(chan struct{})
	go func() {
		defer func() { done <- struct{}{} }()
		index()
	}()

	select {
	case <-done:
		util.Loggrs.Info(uid, "Job 正常完成.")
		return
	case <-ctx.Done():
		// 上下文被取消，可能是超时了
		util.Loggrs.Warn(uid, "Job 发现取消了上下文，可能作业已经超时了.")
		return
	}
}
