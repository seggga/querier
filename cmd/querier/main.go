package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/seggga/querier/config"
	"github.com/seggga/querier/internal/app/csvreader"
)

func main() {
	// load configuration
	config, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	//fmt.Println(config)

	// load users query
	if len(os.Args) < 2 {
		log.Fatal("no query has been passed")
	}
	query := os.Args[1]

	// check the query and create a LexMachine
	lm, err := fillLexMachine(query)
	if err != nil {
		fmt.Println(err)
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
		fmt.Println("Program has been interrupted by user")
	case <-timeOuter.C:
		fmt.Println("there is no time left")
	}
	cancel()

	// graceful shutdown: 3 seconds to close opened csv-files
	timeOuter = time.NewTimer(time.Second * 3)
	select {
	case <-finishChan:
		fmt.Println("all csv-files has been successfully closed")
	case <-timeOuter.C:
		fmt.Println("some csv-files has not been closed")
	}
}
