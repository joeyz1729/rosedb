package rosedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
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

// Reopen the db according to specific config path.
func Reopen(path string) (*RoseDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config
	b, err := ioutil.ReadFile(path + configSaveFile)

	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

// Close RoseDB
func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}

	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil

}

func (db *RoseDB) Sync() error {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, file := range db.activeFile {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *RoseDB) Reclaim() (err error) {
	if db.isSingleReclaiming {
		return ErrDBisReclaiming
	}
	var reclaimable bool
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return ErrReclaimUnreached
	}

	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			if len(db.archFiles[dType]) < db.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds   []int
			)

			for _, file := range db.archFiles[dType] {
				fileIds = append(fileIds, int(file.Id))
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				file := db.archFiles[dType][uint32(fid)]
				var offset int64 = 0
				var reclaimEntries []*storage.Entry

				// read all entries in db file, and find the valid entry.
				for {
					if e, err := file.Read(offset); err == nil {
						if db.validEntry(e, offset, file.Id) {
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("err occurred when read the entry: %+v", err)
						return
					}
				}

				// rewrite the valid entries to new db file.
				for _, entry := range reclaimEntries {
					if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
						df, err = storage.NewDBFile(reclaimPath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}

						if err = df.Write(entry); err != nil {
							log.Fatalf("err occurred when write the entry: %+v", err)
							return
						}

						if dType == String {
							item := db.strIndex.idxList.Get(entry.Meta.Key)
							idx := item.Value().(*index.Indexer)
							idx.Offset = offset
							idx.FileId = fileId
							db.strIndex.idxList.Put(idx.Meta.Key, idx)
						}
					}
				}

			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < DataStructureNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	// delete old db files.
	for dataType, files := range db.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				_ = os.Remove(f.File.Name())
			}
		}
	}

	// copy
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, db.config.DirPath+name)
			}
		}
	}

	db.archFiles = dbArchivedFiles
	return
}

// SingleReclaim only support String type.
func (db *RoseDB) SingleReclaim() (err error) {
	if db.isReclaiming {
		return ErrDBisReclaiming
	}

	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isSingleReclaiming = false
		db.mu.Unlock()
	}()

	db.isSingleReclaiming = true
	var fileIds []int
	for _, file := range db.archFiles[String] {
		fileIds = append(fileIds, int(file.Id))
	}

	sort.Ints(fileIds)

	for _, fid := range fileIds {
		file := db.archFiles[String][uint32(fid)]

		if db.meta.ReclaimableSpace[file.Id] < db.config.SingleReclaimThreshold {
			continue
		}

		var (
			readOff      int64
			validEntries []*storage.Entry
		)

		for {
			entry, err := file.Read(readOff)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if db.validEntry(entry, readOff, uint32(fid)) {
				validEntries = append(validEntries, entry)
			}
			readOff += int64(entry.Size())
		}

		if len(validEntries) == 0 {
			os.Remove(file.File.Name())
			delete(db.meta.ReclaimableSpace, uint32(fid))
			delete(db.archFiles[String], uint32(fid))
			continue
		}

		df, err := storage.NewDBFile(reclaimPath, uint32(fid), db.config.RwMethod, db.config.BlockSize, String)
		if err != nil {
			return err
		}
		for _, e := range validEntries {
			if err := df.Write(e); err != nil {
				return err
			}

			item := db.strIndex.idxList.Get(e.Meta.Key)
			idx := item.Value().(*index.Indexer)
			idx.Offset = df.Offset - int64(e.Size())
			idx.FileId = uint32(fid)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}

		os.Remove(file.File.Name())

		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fid)

		os.Rename(reclaimPath+name, db.config.DirPath+name)

		db.meta.ReclaimableSpace[uint32(fid)] = 0
		db.archFiles[String][uint32(fid)] = df
	}

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
