package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
)

func FilesystemDFS(root string, path string, parentId uint32, idChan chan uint32, reducerChan chan *FSNodeStat, returnChan chan *FSNodeStat, sem *Semaphore, isAsync bool) *FSNodeStat {
	if isAsync {
		sem.Acquire()
		defer sem.Release()
	}

	stat, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		log.Printf("DFS ERROR: Path not found: %s", path)
		if isAsync {
			returnChan <- nil
		}
		return nil
	}
	if err != nil {
		log.Printf("DFS ERROR: Error while getting stats for %s: %s", path, err)
		if isAsync {
			returnChan <- nil
		}
		return nil
	}

	if stat.Mode().IsRegular() {
		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, false, idChan)
		reducerChan <- data
		if isAsync {
			returnChan <- data
		}
		return data
	}

	if stat.Mode()&fs.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			log.Println("DFS ERROR: Symbolic link read error:", err)
		}
		data := CreateFSLinkStat(root, path, parentId, target, idChan)
		reducerChan <- data
		if isAsync {
			returnChan <- data
		}
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
			childrenPaths = append(childrenPaths, path+"/"+child.Name())
		}

		size := stat.Size()
		data := CreateFSNodeStat(root, path, parentId, size, true, idChan)

		if len(childrenPaths) > 0 {
			if len(sem.semC) < sem.maxConcurrency {
				childReturnChan := make(chan *FSNodeStat)
				for _, childPath := range childrenPaths {
					go FilesystemDFS(root, childPath, data.Id, idChan, reducerChan, childReturnChan, sem, true)
				}

				sem.Release()
				for i := 0; i < len(childrenPaths); i++ {
					data.Update(<-childReturnChan)
				}
				sem.Acquire()
			} else {
				for _, childPath := range childrenPaths {
					data.Update(FilesystemDFS(root, childPath, data.Id, idChan, reducerChan, nil, sem, false))
				}
			}
		}

		reducerChan <- data
		if isAsync {
			returnChan <- data
		}
		return data
	}

	log.Println("ERROR: Unsupported file type:", path)
	if isAsync {
		returnChan <- nil
	}
	return nil
}
