package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/seggga/querier/config"
	"github.com/seggga/querier/internal/app/csvreader"
	"github.com/sirupsen/logrus"
)

func main() {
	// load configuration
	config, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	// initialize info logger
	logInfo := logrus.New()
	fileInfo, err := os.OpenFile(config.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer fileInfo.Close()

	logInfo.SetLevel(logrus.InfoLevel)
	logInfo.SetFormatter(&logrus.JSONFormatter{})
	logInfo.SetOutput(fileInfo)
	logInfo.Info("logging started")
	logInfo.Info("command-line parameters:")
	logInfo.Infof("timeout: %s", config.Timeout)
	logInfo.Infof("log-file: %t", config.Log)
	logInfo.Infof("error-file: %s", config.Err)

	// initialize error logger
	logErr := logrus.New()
	fileErr, err := os.OpenFile(config.Err, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer fileErr.Close()

	logErr.SetLevel(logrus.ErrorLevel)
	logErr.SetFormatter(&logrus.JSONFormatter{})
	logErr.SetOutput(fileErr)
	logErr.Info("logging started")

	// load users query
	fmt.Println("Please, enter the query : ")
	reader := bufio.NewReader(os.Stdin)
	query, err := reader.ReadString('\n')
	if err != nil {
		logErr.Errorf("There is an error entering data.\n%v\n", err)
		return
	}
	logInfo.Infof("user's query is: %s", query)

	// check the query and create a LexMachine
	lm, err := fillLexMachine(query)
	if err != nil {
		logErr.Errorln(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	intChan := make(chan os.Signal, 1)
	finishChan := make(chan struct{})
	signal.Notify(intChan, os.Interrupt)
	timeOuter := time.NewTimer(time.Second * time.Duration(config.Timeout))

	go csvreader.ReadTheFile(lm, ctx, finishChan)

	// ctx.cancel function  will be called on INTERRUPT signal or after timeout defined by config
	select {
	case <-intChan:
		logInfo.Println("Program has been interrupted by user")
	case <-timeOuter.C:
		logInfo.Println("there is no time left")
	}
	cancel()

	// graceful shutdown: 3 seconds to close opened csv-files
	timeOuter = time.NewTimer(time.Second * 3)
	select {
	case <-finishChan:
		logInfo.Println("all csv-files has been successfully closed")
	case <-timeOuter.C:
		logInfo.Println("some csv-files has not been closed")
	}
}
