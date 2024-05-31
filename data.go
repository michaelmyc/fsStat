package main

import (
	"fmt"
	"strings"
)

type FSNodeStat struct {
	Id           uint32
	ParentId     uint32
	Path         string
	IsDir        bool
	Size         uint64
	Count        uint32
	SFileCount   uint32
	SFileSize    uint64
	MFileCount   uint32
	MFileSize    uint64
	LFileCount   uint32
	LFileSize    uint64
	XLFileCount  uint32
	XLFileSize   uint64
	XXLFileCount uint32
	XXLFileSize  uint64
}

func (f *FSNodeStat) Update(child *FSNodeStat) {
	f.Size += child.Size
	f.Count += child.Count
	f.SFileCount += child.SFileCount
	f.SFileSize += child.SFileSize
	f.MFileCount += child.MFileCount
	f.MFileSize += child.MFileSize
	f.LFileCount += child.LFileCount
	f.LFileSize += child.LFileSize
	f.XLFileCount += child.XLFileCount
	f.XLFileSize += child.XLFileSize
	f.XXLFileCount += child.XXLFileCount
	f.XXLFileSize += child.XXLFileSize
}

func (f *FSNodeStat) String() string {
	return fmt.Sprintf(
		`Id: %d
ParentId: %d
Path: %s
IsDir: %t
Count: %d
Size: %s

S Files - less than 512KiB
--------------------------
Count: %d
Size: %s

M Files - 512KiB ~ 4MiB
-----------------------
Count: %d
Size: %s

L Files - 4MiB ~ 50MiB
----------------------
Count: %d
Size: %s

XL Files - 50MiB ~ 200MiB
-------------------------
Count: %d
Size: %s

XXL Files - larger than 200MiB
------------------------------
Count: %d
Size: %s`,
		f.Id,
		f.ParentId,
		f.Path,
		f.IsDir,
		f.Count,
		toAppropriateUnit(f.Size),
		f.SFileCount,
		toAppropriateUnit(f.SFileSize),
		f.MFileCount,
		toAppropriateUnit(f.MFileSize),
		f.LFileCount,
		toAppropriateUnit(f.LFileSize),
		f.XLFileCount,
		toAppropriateUnit(f.XLFileSize),
		f.XXLFileCount,
		toAppropriateUnit(f.XXLFileSize),
	)
}

func unrootPath(path string, root string) string {
	unrootedPath := strings.TrimPrefix(path, root)
	return "/" + strings.Trim(unrootedPath, "/")
}

func CreateFSNodeStat(root string, path string, parentId uint32, size int64, isDir bool, idChan chan uint32) *FSNodeStat {
	data := FSNodeStat{
		Id:           <-idChan,
		ParentId:     parentId,
		Path:         unrootPath(path, root),
		IsDir:        isDir,
		Size:         uint64(size),
		Count:        0,
		SFileCount:   0,
		SFileSize:    0,
		MFileCount:   0,
		MFileSize:    0,
		LFileCount:   0,
		LFileSize:    0,
		XLFileCount:  0,
		XLFileSize:   0,
		XXLFileCount: 0,
		XXLFileSize:  0,
	}

	fileCount := uint32(1)
	if isDir {
		fileCount = 0
	}
	data.Count = fileCount

	switch {
	case data.Size <= 512*1024: // 0 to 512 KiB
		data.SFileCount = fileCount
		data.SFileSize = data.Size
	case data.Size >= 512*1024 && data.Size < 4*1024*1024: // 512 KiB to 4 MiB
		data.MFileCount = fileCount
		data.MFileSize = data.Size
	case data.Size >= 4*1024*1024 && data.Size < 50*1024*1024: // 4 MiB to 50 MiB
		data.LFileCount = fileCount
		data.LFileSize = data.Size
	case data.Size >= 50*1024*1024 && data.Size < 200*1024*1024: // 50 MiB to 200 MiB
		data.XLFileCount = fileCount
		data.XLFileSize = data.Size
	case data.Size > 200*1024*1024: // above 200 MiB
		data.XXLFileCount = fileCount
		data.XXLFileSize = data.Size
	}

	return &data
}

func toAppropriateUnit(size uint64) string {
	if size < 1024 {
		return fmt.Sprintf("%d B", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%.2f KiB", float64(size)/1024)
	} else if size < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MiB", float64(size)/(1024*1024))
	} else if size < 1024*1024*1024*1024 {
		return fmt.Sprintf("%.2f GiB", float64(size)/(1024*1024*1024))
	} else {
		return fmt.Sprintf("%.2f TiB", float64(size)/(1024*1024*1024*1024))
	}
}
