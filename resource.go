package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type ResourceUsage struct {
	MaxSystemMemory uint64
	MaxGoroutines   int
	Duration        time.Duration
}

func MonitorResources(startTime time.Time, returnChan chan *ResourceUsage, end chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	maxSystemMemory := uint64(0)
	maxGoroutines := 0
	for {
		select {
		case <-end:
			returnChan <- &ResourceUsage{maxSystemMemory, maxGoroutines, time.Since(startTime)}
			return
		default:
			maxSystemMemory = max(maxSystemMemory, GetSystemMemory())
			maxGoroutines = max(maxGoroutines, runtime.NumGoroutine())
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func ReportResourceUsage(resources *ResourceUsage) {
	fmt.Println()
	fmt.Println("======= Resource Usage =======")
	fmt.Println("Duration:", resources.Duration.String())
	fmt.Println("Max system memory:", toAppropriateUnit(resources.MaxSystemMemory))
	fmt.Println("Max goroutines:", resources.MaxGoroutines)
}

func GetSystemMemory() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}
