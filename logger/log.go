package log

import "github.com/sirupsen/logrus"

var (
	logger = logrus.New()
)

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}
