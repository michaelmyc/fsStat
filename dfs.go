package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
)

func AsyncDFS(root string, path string, parentId uint32, idChan chan uint32, writerChan chan *FSNodeStat, returnChan chan *FSNodeStat, sem *Semaphore, depth int, asyncDepth int) {
	sem.Acquire()
	defer sem.Release()

	stat, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Printf("DFS ERROR: Path not found: %s", path)
		returnChan <- nil
		return
	}
	if err != nil {
		log.Printf("DFS ERROR: Error while getting stats for %s: %s", path, err)
		returnChan <- nil
		return
	}

	if stat.Mode().IsRegular() {
		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, false, idChan)
		writerChan <- data
		returnChan <- data
		return
	}

	if stat.Mode()&fs.ModeSymlink != 0 {
		log.Println("WARNING: Symbolic link skipped:", path)
		returnChan <- nil
		return
	}

	if stat.IsDir() {
		var childrenPaths []string
		children, err := os.ReadDir(path)
		if err != nil {
			log.Printf("DFS ERROR: Error while reading directory %s: %s", path, err)
		}
		childrenPaths = []string{}
		for _, child := range children {
			if child.Type().IsRegular() || child.Type().IsDir() {
				childrenPaths = append(childrenPaths, path+"/"+child.Name())
			}
		}

		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, true, idChan)

		if len(childrenPaths) > 0 {
			if depth < asyncDepth {
				childReturnChan := make(chan *FSNodeStat)
				for _, childPath := range childrenPaths {
					go AsyncDFS(root, childPath, data.Id, idChan, writerChan, childReturnChan, sem, depth+1, asyncDepth)
				}

				sem.Release()
				for i := 0; i < len(childrenPaths); i++ {
					data.Update(<-childReturnChan)
				}
				sem.Acquire()
			} else {
				for _, childPath := range childrenPaths {
					data.Update(SyncDFS(root, childPath, data.Id, idChan, writerChan))
				}
			}
		}

		writerChan <- data
		returnChan <- data
		return
	}

	log.Println("ERROR: Unsupported file type:", path)
}

func SyncDFS(root string, path string, parentId uint32, idChan chan uint32, writerChan chan *FSNodeStat) *FSNodeStat {
	stat, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Printf("DFS ERROR: Path not found: %s", path)
		return nil
	}
	if err != nil {
		log.Printf("DFS ERROR: Error while getting stats for %s: %s", path, err)
		return nil
	}

	if stat.Mode()&fs.ModeSymlink != 0 {
		log.Println("WARNING: Symbolic link skipped:", path)
		return nil
	}

	if stat.Mode().IsRegular() {
		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, false, idChan)
		writerChan <- data
		return data
	}

	if stat.IsDir() {
		var childrenPaths []string
		children, err := os.ReadDir(path)
		if err != nil {
			log.Printf("DFS ERROR: Error while reading directory %s: %s", path, err)
		}
		childrenPaths = []string{}
		for _, child := range children {
			if child.Type().IsRegular() || child.Type().IsDir() {
				childrenPaths = append(childrenPaths, path+"/"+child.Name())
			}
		}

		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, true, idChan)

		if len(childrenPaths) > 0 {
			for _, childPath := range childrenPaths {
				data.Update(SyncDFS(root, childPath, data.Id, idChan, writerChan))
			}
		}

		writerChan <- data
		return data
	}

	log.Println("ERROR: Unsupported file type:", path)
	return nil
}
