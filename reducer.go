package main

import (
	"database/sql"
	"log"
	"sync"
	"time"
)

func skipCriteria(data *FSNodeStat, saveAllFiles bool) bool {
	if saveAllFiles {
		return false
	}
	return !data.IsDir && !data.IsSymlink && data.Size <= 200*1024*1024
}

func Reducer(db *sql.DB, bufferSize int, loggingInterval int, saveAllFiles bool, startTime time.Time, dataChan chan *FSNodeStat, end chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	totalCount := uint64(0)
	totalSize := uint64(0)

	lastCheckpointSize := uint64(0)
	lastCheckpointTime := time.Now()

	buffer := make([]*FSNodeStat, bufferSize)
	count := 0
	ending := false
	for {
		select {
		case data := <-dataChan:
			totalCount++
			totalSize += data.SelfSize

			if (totalCount)%uint64(loggingInterval) == 0 {
				thisCheckpointTime := time.Now()
				sizeIncrement := totalSize - lastCheckpointSize
				timeIncrement := thisCheckpointTime.Sub(lastCheckpointTime)
				timeElapsed := thisCheckpointTime.Sub(startTime)
				currentSpeed := CalculateSpeed(sizeIncrement, timeIncrement.Seconds())
				averageSpeed := CalculateSpeed(totalSize, timeElapsed.Seconds())

				log.Printf("===== Checkpoint =====")
				log.Printf("Data in queue: %v", len(dataChan))
				log.Printf("Memory used: %s", toAppropriateUnit(GetSystemMemory()))
				log.Printf("Current file node: %s", data.Path)
				log.Printf("File nodes scanned: %d", totalCount)
				log.Printf("Size scanned: %s", toAppropriateUnit(totalSize))
				log.Printf("Time elapsed: %s", timeElapsed.String())
				log.Printf("Current speed: %s/s", toAppropriateUnit(currentSpeed))
				log.Printf("Average speed: %s/s", toAppropriateUnit(averageSpeed))

				lastCheckpointSize = totalSize
				lastCheckpointTime = thisCheckpointTime
			}

			if skipCriteria(data, saveAllFiles) {
				continue
			}

			buffer[count] = data
			count++
			if count == bufferSize {
				BatchInsertData(buffer[:count], db)
				count = 0
			}
		case <-end:
			ending = true
		default:
			if ending {
				BatchInsertData(buffer[:count], db)
				return
			}
		}
	}
}

func CalculateSpeed(bytes uint64, duration float64) uint64 {
	if duration == 0 {
		return 0
	}
	return uint64(float64(bytes) / duration)
}
