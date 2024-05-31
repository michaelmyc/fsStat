package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
)

func main() {
	dbPath := flag.String("db", "fs_stats.db", "Location of the database file")
	bufferSize := flag.Int("buffer", 200, "Size of the buffer for writing to the database")
	concurrency := flag.Int("concurrency", 128, "Maximum amount of concurrent workers")
	skipConfirmation := flag.Bool("y", false, "Whether to skip confirmation prompt")
	flag.Parse()

	var root string
	if len(flag.Args()) > 1 {
		log.Fatalln("Too many arguments")
	} else if len(flag.Args()) == 0 {
		root = "."
	} else {
		root = flag.Arg(0)
	}

	log.Println("Starting fsStat on", root)
	log.Println("Database location:", *dbPath)
	log.Println("Buffer size:", *bufferSize)
	log.Println("Concurrency:", *concurrency)

	db, err := ConnectDB(*dbPath, *skipConfirmation)
	if err != nil {
		log.Fatalln("Failed to connect to database:", err)
	}
	defer db.Close()

	writerChan := make(chan *FSNodeStat, *concurrency) // use buffered channel to prevent blocking
	returnChan := make(chan *FSNodeStat)
	idChan := make(chan uint32)
	endChan := make(chan bool)

	sem := CreateSemaphore(*concurrency)
	wg := new(sync.WaitGroup)

	go DBWriter(db, *bufferSize, writerChan, endChan, wg)
	go IdGenerator(1, idChan)
	go AsyncDFS(root, root, 0, idChan, writerChan, returnChan, sem)
	totalSize := <-returnChan
	endChan <- true
	wg.Wait()
	fmt.Println()
	fmt.Println("=========== Summary ===========")
	fmt.Println(totalSize.String())
}
