package rosedb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"zouyi/rosedb/storage"
)

type (
	RoseDB struct {
		activeFile    ActiveFiles // active data file
		activeFileIds ActiveFileIds
		archFiles     ArchivedFiles // already archived data file f

		strIndex  *StrIdx
		listIndex *ListIdx
		hashIndex *HashIdx
		setIndex  *SetIdx
		zsetIndex *ZsetIdx

		config Config

		meta *storage.Meta

		expires            Expires
		isReclaiming       bool
		isSingleReclaiming bool
		mu                 sync.Mutex
	}

	ActiveFiles map[DataType]*storage.DBFile

	ActiveFileIds map[DataType]uint32

	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	Expires map[DataType]map[string]int64
)

// Open a database project
func Open(dirPath string) (*RoseDB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := storage.NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	db.loadIndexesFromFile()
	return db, nil
}

// loadIndexesFromFile
func (db *RoseDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
	return
}

// Close RoseDB file
func (db *RoseDB) Close() error {
	if db.dbFile == nil {
		return errors.New("invalid db file")
	}
	return db.dbFile.File.Close()
}

func (db *RoseDB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		entry, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if off, ok := db.indexes[string(entry.Key)]; ok && off == offset {
			validEntries = append(validEntries, entry)
		}
		offset += entry.GetSize()
	}

	if len(validEntries) > 0 {
		// create new temporary file
		mergeDbFile, err := storage.NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDbFile.File.Name())

		db.mu.Lock()
		defer db.mu.Unlock()

		for _, entry := range validEntries {
			// write valid entry into new file
			writeOff := mergeDbFile.Offset
			err := mergeDbFile.Write(entry)
			if err != nil {
				return err
			}

			// update entry index
			db.indexes[string(entry.Key)] = writeOff
		}

		// remove old db file
		dbFilename := db.dbFile.File.Name()
		db.dbFile.File.Close()
		os.Remove(dbFilename)

		// rename new  db file
		mergeDbFilename := mergeDbFile.File.Name()
		os.Rename(mergeDbFilename, filepath.Join(db.dirPath, storage.Filename))

		// replace db file
		db.dbFile = mergeDbFile
	}

	return nil
}

func (db *RoseDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// create entry record
	offset := db.dbFile.Offset
	entry := NewEntry(key, value, PUT)

	// append to db file
	err = db.dbFile.Write(entry)

	// write into storage
	db.indexes[string(key)] = offset
	return
}

func (db *RoseDB) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset, ok := db.indexes[string(key)]

	if !ok {
		err = fmt.Errorf("{key: %s doesn't exist.}", key)
		return
	}

	var entry *Entry
	entry, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if entry != nil {
		value = entry.Value
	}
	return
}

func (db *RoseDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, ok := db.indexes[string(key)]
	if !ok {
		return
	}

	entry := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(entry)
	if err != nil {
		return
	}
	delete(db.indexes, string(key))
	return
}
