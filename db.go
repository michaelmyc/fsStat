package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

func skipCriteria(data *FSNodeStat) bool {
	// less than or equal to 200MB files
	return !data.IsDir && data.Size <= 200*1024*1024
}

func DBWriter(db *sql.DB, bufferSize int, loggingInterval int, dataChan chan *FSNodeStat, end chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	totalCount := uint64(0)
	totalSize := uint64(0)

	buffer := make([]*FSNodeStat, bufferSize)
	count := 0
	ending := false
	for {
		select {
		case data := <-dataChan:
			if (totalCount+1)%uint64(loggingInterval) == 0 {
				log.Printf("Current file node: %s", data.Path)
				log.Printf("File nodes scanned: %d", totalCount+1)
				log.Printf("Size scanned: %s", toAppropriateUnit(totalSize+data.Size))
			}
			if skipCriteria(data) {
				totalCount++
				totalSize += data.Size
				continue
			}
			buffer[count] = data
			count++
			totalCount++
			totalSize += data.Size
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

func removeDB(path string) {
	log.Printf("Removing database at %s", path)
	os.Remove(path)
}

func RemoveDBIfAllowed(path string, skipConfirmation bool) {
	fmt.Print("Do you want to overwrite it? ([y]/n): ")

	if skipConfirmation {
		fmt.Println()
		removeDB(path)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	option, _ := reader.ReadString('\n')
	switch option {
	case "\n":
		removeDB(path)
	case "y\n", "Y\n":
		removeDB(path)
	case "n\n", "N\n":
		log.Fatalln("Aborting...")
	default:
		fmt.Println(option)
		log.Println("Invalid option")
		RemoveDBIfAllowed(path, skipConfirmation)
	}
}

func ConnectDB(path string, skipConfirmation bool) (*sql.DB, error) {
	var db *sql.DB

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		log.Printf("Creating new database at %s", path)
	} else if err != nil {
		log.Fatalf("Error when checking file: %v", err)
	} else {
		log.Printf("WARNING: Database %s is already present", path)
		RemoveDBIfAllowed(path, skipConfirmation)
	}

	db, err = sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE FileNodes (
		Id INTEGER PRIMARY KEY,
		ParentId INTEGER,
		Path TEXT,
		IsDir BOOLEAN,
		Size INTEGER,
		Count INTEGER,
		SFileCount INTEGER,
		SFileSize INTEGER,
		MFileCount INTEGER,
		MFileSize INTEGER,
		LFileCount INTEGER,
		LFileSize INTEGER,
		XLFileCount INTEGER,
		XLFileSize INTEGER,
		XXLFileCount INTEGER,
		XXLFileSize INTEGER
	)`)
	if err != nil {
		log.Fatal(err)
	}

	return db, nil
}

func CloseDB(db *sql.DB) {
	defer db.Close()
}

func BatchInsertData(data []*FSNodeStat, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(`INSERT INTO 
	FileNodes(
		Id, 
		ParentId, 
		Path, 
		IsDir, 
		Size, 
		Count, 
		SFileCount, 
		SFileSize, 
		MFileCount, 
		MFileSize, 
		LFileCount, 
		LFileSize, 
		XLFileCount, 
		XLFileSize, 
		XXLFileCount, 
		XXLFileSize
	) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, datum := range data {
		_, err := stmt.Exec(
			datum.Id,
			datum.ParentId,
			datum.Path,
			datum.IsDir,
			datum.Size,
			datum.Count,
			datum.SFileCount,
			datum.SFileSize,
			datum.MFileCount,
			datum.MFileSize,
			datum.LFileCount,
			datum.LFileSize,
			datum.XLFileCount,
			datum.XLFileSize,
			datum.XXLFileCount,
			datum.XXLFileSize,
		)
		if err != nil {
			tx.Rollback() // roll back if failed
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
