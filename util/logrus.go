/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package util
 *@file    logrus
 *@date    2024/11/6 18:59
 */

package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)
import "github.com/sirupsen/logrus"

// MyFormatter 是自定义的logrus.Formatter，它仅显示文件名和行号
type MyFormatter struct {
	logrus.TextFormatter
}

func Logrus() {
	// 创建日志文件
	filename := fmt.Sprintf("%s/%s.log", Config.GetString("logger.LogPath"), filepath.Base(os.Args[0]))
	logFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logrus.Fatalf("Failed to open log file: %v", err)
	}
	// 设置日志输出为文件和标准输出
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	Loggrs = &logrus.Logger{
		// 设置日志输出为文件和标准输出
		Out: multiWriter,
		Formatter: &MyFormatter{
			TextFormatter: logrus.TextFormatter{
				// 显示颜色级别
				ForceColors:               true,
				ForceQuote:                true,
				EnvironmentOverrideColors: true,
				// 显示具体的时间
				FullTimestamp:    true,
				PadLevelText:     true,
				QuoteEmptyFields: true,
				CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
					// 从frame的文件名中移除路径部分，只保留文件名和行号
					fileName := frame.File
					// 使用string索引找到最后一个'/'的位置，然后切割字符串
					file = fmt.Sprintf("  %v:%d", fileName[strings.LastIndex(fileName, "/")+1:], frame.Line)
					//function = frame.Function
					width := 35
					file = fmt.Sprintf("%-*s", width, file)

					return
				},
			},
		},
		// 设置日志级别
		Level: logrus.DebugLevel,
		// 启用报告调用者信息，显示日志打印的文件名和行号
		ReportCaller: true,
	}

}
