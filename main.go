package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
)

func main() {
	dbPath := flag.String("db", "fs_stats.db", "Location of the database file")
	bufferSize := flag.Int("buffer", 256, "Size of the buffer for writing to the database")
	concurrency := flag.Int("concurrency", 128, "Maximum amount of concurrent workers")
	asyncDepth := flag.Int("asyncDepth", 5, "Depth of asynchronous DFS before switching to synchronous")
	skipConfirmation := flag.Bool("y", false, "Whether to skip confirmation prompt")
	loggingInterval := flag.Int("interval", 5000, "How often to log progress")
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

	writerChan := make(chan *FSNodeStat, *bufferSize) // use buffered channel to prevent blocking
	idChan := make(chan uint32)

	dfsReturnChan := make(chan *FSNodeStat)
	monitorReturnChan := make(chan *ResourceUsage)

	monitorEndChan := make(chan bool)
	writerEndChan := make(chan bool)

	sem := CreateSemaphore(*concurrency)
	wg := new(sync.WaitGroup)

	go MonitorResources(monitorReturnChan, monitorEndChan, wg)

	go DBWriter(db, *bufferSize, *loggingInterval, writerChan, writerEndChan, wg)
	go IdGenerator(1, idChan)
	go AsyncDFS(root, root, 0, idChan, writerChan, dfsReturnChan, sem, 0, *asyncDepth)
	nodeStats := <-dfsReturnChan
	writerEndChan <- true
	monitorEndChan <- true
	resourceUsage := <-monitorReturnChan
	wg.Wait()
	fmt.Println("Done")
	if nodeStats != nil {
		fmt.Println()
		fmt.Println("=========== Summary ===========")
		fmt.Println(nodeStats.String())
	}
	ReportResourceUsage(resourceUsage)
}
