/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    emo_progressbar
 *@date    2024/11/8 17:37
 */

package fronends

import (
	"fmt"
	"strings"
	"time"
)

func progressbar() {
	var doneC = make(chan int)
	go func() {
		for {
			select {
			case <-doneC:
				PrintProgress(<-doneC, 120)
			}
		}
	}()

	for i := 0; i < 120; i++ {
		doneC <- i
		time.Sleep(time.Second * 1)
	}
}

// PrintProgress 用于在一行内打印进度条
func PrintProgress(current, total int) {
	// 计算进度百分比
	percent := int(float64(current) / float64(total) * 100)
	// 创建进度条
	bar := strings.Repeat("-", percent) + strings.Repeat(" ", 100-percent)
	// 使用ANSI转义序列将光标移动到行首
	fmt.Printf("\033[2K\r%d%% [%s](%d/%d)", percent, bar, current, total)
}
