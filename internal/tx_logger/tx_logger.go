package tx_logger

import (
	"fmt"
	"os"
	"path/filepath"
)

type TXLogger struct {
	LogPath string
	LogChan chan string
}

func NewTXLogger(path string) *TXLogger {
	txLogger := new(TXLogger)
	txLogger.LogPath = path
	txLogger.LogChan = make(chan string)

	checkLogFileExists(path)
	return txLogger
}

func checkLogFileExists(path string) {
	rootDir, _ := os.Getwd()
	logPath := filepath.Join(rootDir, "tx_logs")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		os.Mkdir(logPath, 0700)
	}

	logFile := filepath.Join(logPath, path)
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	}
}

func (txl *TXLogger) ProcessLogWrite() {
	projectDir, _ := os.Getwd()
	file, err := os.OpenFile(filepath.Join(projectDir, "tx_logs", txl.LogPath), os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("Can not write logs to logfile: ", err)
		return
	}
	defer file.Close()

	for {
		select {
		case logInfo := <-txl.LogChan:
			fmt.Println(logInfo)
			_, err := file.Write([]byte(logInfo))
			if err != nil {
				fmt.Println("error writing transaction log  to file:  ", err)
			}
		}
	}
}
