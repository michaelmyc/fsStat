package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
)

func AsyncDFS(root string, path string, parentId uint32, idChan chan uint32, writerChan chan *FSNodeStat, returnChan chan *FSNodeStat, sem *Semaphore) {
	sem.Acquire()
	defer sem.Release()

	stat, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Fatalln("Path not found:", path)
	}
	if err != nil {
		log.Fatalln("Error while getting stats for", path, ":", err)
	}

	if stat.Mode().IsRegular() {
		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, false, idChan)
		writerChan <- data
		returnChan <- data
		return
	}

	var childrenPaths []string
	if stat.IsDir() {
		children, err := os.ReadDir(path)
		if err != nil {
			log.Fatalln("Error while reading directory", path, ":", err)
		}
		childrenPaths = []string{}
		for _, child := range children {
			if child.Type().IsRegular() || child.Type().IsDir() {
				childrenPaths = append(childrenPaths, path+"/"+child.Name())
			}
		}

		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, true, idChan)
		sem.Release()

		if len(childrenPaths) > 0 {
			childReturnChan := make(chan *FSNodeStat)
			for _, childPath := range childrenPaths {
				go AsyncDFS(root, childPath, data.Id, idChan, writerChan, childReturnChan, sem)
			}

			for i := 0; i < len(childrenPaths); i++ {
				data.Update(<-childReturnChan)
			}
		}

		sem.Acquire()

		writerChan <- data
		returnChan <- data
		return
	}

	if stat.Mode()&fs.ModeSymlink != 0 {
		log.Println("WARNING: Symbolic link found:", path)
	}

	log.Println("ERROR: Unsupported file type:", path)
}
