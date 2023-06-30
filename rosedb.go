package rosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"zouyi/rosedb/index"
	"zouyi/rosedb/storage"
	"zouyi/rosedb/utils"
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

		meta *storage.DBMeta

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
func Open(config Config) (*RoseDB, error) {
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &RoseDB{
		activeFile:    activeFiles,
		activeFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		meta:          meta,

		strIndex:  newStrIdx(),
		listIndex: newListIdx(),
		hashIndex: newHashIdx(),
		setIndex:  newSetIdx(),
		zsetIndex: newZsetIdx(),

		expires: make(Expires),
	}

	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	if err := db.loadIndexesFromFiles(); err != nil {
		return nil, err
	}
	return db, nil
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

// buildIndex build the indexes for different data structure.
func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer) error {
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(idx, entry)
	case storage.Hash:
		db.buildHashIndex(idx, entry)
	case storage.Set:
		db.buildSetIndex(idx, entry)
	case storage.ZSet:
		db.buildSetIndex(idx, entry)
	}
	return nil
}

// Backup copy the db dir for backup
func (db *RoseDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

func (db *RoseDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return ErrEmptyKey
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

func (db *RoseDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

func (db *RoseDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

func (db *RoseDB) store(e *storage.Entry) error {
	config := db.config
	if db.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}

		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}

	if err := db.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}

	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset

	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	mark := e.GetMark()
	switch e.GetType() {
	case String:
		deadline, exist := db.expires[String][string(e.Meta.Key)]
		now := time.Now().Unix()

		if mark == StringExpire {
			if exist && deadline > now {
				return true
			}
		}
		if mark == StringSet || mark == StringPersist {
			if exist && deadline <= now {
				return false
			}

			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer != nil && indexer.FileId == fileId && indexer.Offset == offset {
					return true
				}
			}
		}
	case List:
		if mark == ListLExpire {
			deadline, exist := db.expires[List][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}

		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case Hash:
		if mark == HashHExpire {
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSExpire {
			deadline, exist := db.expires[Set][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}
		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZExpire {
			deadline, exist := db.expires[ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score != val {
					return true
				}
			}
		}
	}
	return false
}

func (db *RoseDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline {
		expired = true

		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			if ele := db.strIndex.idxList.Remove(key); ele != nil {
				db.incrReclaimableSpace(key)
			}
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			var l *list.List
			l = db.listIndex.indexes
			l.LClear(string(key))
			db.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, Set, SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case ZSet:
			e = storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
			db.zsetIndex.indexes.ZClear(string(key))
		}
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		// delete the expired info stored at key.
		delete(db.expires[dType], string(key))
	}
	return
}
