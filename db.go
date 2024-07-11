package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func removeDB(path string) {
	log.Printf("Removing database at %s", path)
	os.Remove(path)
}

func RemoveDBIfAllowed(path string, skipConfirmation bool) {
	fmt.Print("Do you want to overwrite it? ([y]/n): ")

	if skipConfirmation {
		fmt.Println("y")
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
		LinkTarget TEXT,
		Size INTEGER,
		Count INTEGER,
		SymlinkCount INTEGER,
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
		LinkTarget,
		Size,
		Count,
		SymlinkCount,
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
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
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
			datum.LinkTarget,
			datum.Size,
			datum.Count,
			datum.SymlinkCount,
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

func CreateIndex(db *sql.DB) {
	_, err := db.Exec(`CREATE INDEX Index_ParentId ON FileNodes(ParentId)`)
	if err != nil {
		log.Fatal(err)
	}
}
