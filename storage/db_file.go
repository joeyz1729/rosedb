package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	mmap "mmap-go"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	// FilePerm default file permission
	FilePerm = 0644

	// PathSeparator default path separator
	PathSeparator = string(os.PathSeparator)
)

var (
	// DBFileFormatNames default format of data file name.
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	// DBFileSuffixName represent the suffix names of data files
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the key of entry is empty")
)

type FileRWMethod uint8

const (
	FileID FileRWMethod = iota
	MMap
)

type DBFile struct {
	Id   uint32
	path string
	mmap mmap.MMap

	File   *os.File
	Offset int64

	method FileRWMethod
}

// NewDBFile create a new db file
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFile{
		Id:     fileId,
		path:   path,
		Offset: 0,
		method: method,
	}

	if method == FileID {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

// Read db file from offset, return entry record
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return nil, err
	}
	if e, err = Decode(buf); err != nil {
		return nil, err
	}

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return nil, err
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return nil, err
		}
		e.Meta.Value = val
	}

	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return nil, err
		}
		e.Meta.Extra = extra
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

func (df *DBFile) readBuf(offset int64, size int64) ([]byte, error) {
	buf := make([]byte, size)

	if df.method == FileID {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil

}

// Write entry into data file from offset
func (df *DBFile) Write(e *Entry) (err error) {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileID {
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}
	if method == MMap {
		copy(df.mmap[writeOff:], encVal)
	}
	df.Offset = int64(e.Size())
	return nil
}

// Close data file after write and read
func (df *DBFile) Close(sync bool) (err error) {
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}

	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return
}

// Sync persist data to stable storage
func (df *DBFile) Sync() (err error) {
	if df.File != nil {
		err = df.File.Sync()
	}

	if df.mmap != nil {
		err = df.mmap.Flush()
	}

	return
}

// Build data file.
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			// find the different types of file.
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	// load all the data files
	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIds := fileIdsMap[dataType]
		sort.Ints(fileIds)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0

		if len(fileIds) > 0 {
			activeFileId = uint32(fileIds[len(fileIds)-1])

			for i := 0; i < len(fileIds)-1; i++ {
				id := fileIds[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}
