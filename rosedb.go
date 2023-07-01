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
		// 活跃文件以及索引
		activeFile    ActiveFiles
		activeFileIds ActiveFileIds
		// 封存文件
		archFiles ArchivedFiles
		// 各类数据类型的索引
		strIndex  *StrIdx
		listIndex *ListIdx
		hashIndex *HashIdx
		setIndex  *SetIdx
		zsetIndex *ZsetIdx
		// 数据库配置
		config Config
		// 其他
		meta               *storage.DBMeta
		expires            Expires
		isReclaiming       bool
		isSingleReclaiming bool
		mu                 sync.Mutex
	}
	// ActiveFiles 数据类型对应的数据库文件
	ActiveFiles map[DataType]*storage.DBFile
	// ActiveFileIds 数据类型对应的文件索引
	ActiveFileIds map[DataType]uint32
	// ArchivedFiles 数据类型对应的索引，对应的数据文件
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile
	// Expires 各类数据类型的过期时间
	Expires map[DataType]map[string]int64
)

// Open a database project
func Open(config Config) (*RoseDB, error) {
	// 路径
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 加载文件
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}
	activeFiles := make(ActiveFiles)

	// 根据各类数据类型活跃文件与索引，创建活跃文件
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	// 读取meta信息
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	// 更新文件的offset
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}
	// 初始化创建数据库
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
	// 初始化文件过期时间
	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	// 加载文件索引
	if err := db.loadIndexesFromFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// Reopen the db according to specific config path.
// 根据路径下的配置文件重新加载数据库
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

	// 保存配置和元数据
	if err := db.saveConfig(); err != nil {
		return err
	}
	if err := db.saveMeta(); err != nil {
		return err
	}

	// 关闭活跃文件
	for _, file := range db.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	// 档案文件刷盘
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Sync
// 将活跃文件刷盘
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

// Reclaim
// *重要， 重新组织磁盘中的数据，回收磁盘空间
func (db *RoseDB) Reclaim() (err error) {
	// 如果single reclaim 正在进行，则不能执行reclaim操作
	if db.isSingleReclaiming {
		return ErrDBisReclaiming
	}
	// 检查是否到达reclaim阈值
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
	// 准备reclaim， 创建临时文件
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)
	// 加锁并修改参数表示正在进行reclaim，防止多个进程重复reclaim
	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	// 并发同时对各类数据类型处理
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

						// String跳表
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

// SingleReclaim 根据Threshold重新组织
// 目前只实现了String类型
func (db *RoseDB) SingleReclaim() (err error) {
	if db.isReclaiming {
		return ErrDBisReclaiming
	}

	// 创建临时用语重新组织的文件，结束后移除
	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)
	// 上锁
	db.mu.Lock()
	defer func() {
		db.isSingleReclaiming = false
		db.mu.Unlock()
	}()
	db.isSingleReclaiming = true
	// 准备整理已封存文件
	var fileIds []int
	for _, file := range db.archFiles[String] {
		fileIds = append(fileIds, int(file.Id))
	}
	// 按照索引排序
	sort.Ints(fileIds)

	for _, fid := range fileIds {
		file := db.archFiles[String][uint32(fid)]
		// 直到阈值，
		if db.meta.ReclaimableSpace[file.Id] < db.config.SingleReclaimThreshold {
			continue
		}
		// 数据偏移位和entry列
		var (
			readOff      int64
			validEntries []*storage.Entry
		)

		for {
			// 从offset读取entry
			entry, err := file.Read(readOff)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 检查entry合法性
			if db.validEntry(entry, readOff, uint32(fid)) {
				validEntries = append(validEntries, entry)
			}
			// 更新offset值
			readOff += int64(entry.Size())
		}
		// 如果没有合法entry，将非法file和fid删除
		if len(validEntries) == 0 {
			os.Remove(file.File.Name())
			delete(db.meta.ReclaimableSpace, uint32(fid))
			delete(db.archFiles[String], uint32(fid))
			continue
		}
		// 创建文件准备reclaim
		df, err := storage.NewDBFile(reclaimPath, uint32(fid), db.config.RwMethod, db.config.BlockSize, String)
		if err != nil {
			return err
		}
		// entry写入进文件
		for _, e := range validEntries {
			if err := df.Write(e); err != nil {
				return err
			}
			// 更新String索引
			item := db.strIndex.idxList.Get(e.Meta.Key)
			idx := item.Value().(*index.Indexer)
			idx.Offset = df.Offset - int64(e.Size())
			idx.FileId = uint32(fid)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}
		// 删除旧文件，将临时文件修改为新封存文件
		os.Remove(file.File.Name())
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fid)
		os.Rename(reclaimPath+name, db.config.DirPath+name)
		// 更新数据库信息
		db.meta.ReclaimableSpace[uint32(fid)] = 0
		db.archFiles[String][uint32(fid)] = df
	}
	return
}

// Backup copy the db dir for backup
// 备份db文件到指定目录下
func (db *RoseDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
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

// validEntry 判断entry条目是否合法
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

// checkKeyValue 检查key-value是否正确
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

// checkExpired 检查特定数据类型的key对应value值是否过期
func (db *RoseDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}

	if time.Now().Unix() > deadline {
		expired = true

		// 判断key-value值已经过期，需要根据不同的数据类型来删除文件
		// 创建Entry，因为数据库存储是日志类型
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
		// 存储entry
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}
		// delete the expired info stored at key.
		delete(db.expires[dType], string(key))
	}
	return
}

// saveConfig 将db的config配置保存到path文件中
func (db *RoseDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()
	return
}

// saveMeta 将db的meta数据保存到path文件中
func (db *RoseDB) saveMeta() error {
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

// store 将entry条目进行存储
func (db *RoseDB) store(e *storage.Entry) error {
	config := db.config
	// 判断是否超界
	// 如果超界， 需要将entry分成多个文件存储
	if db.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		// 活跃文件刷盘
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
		// 将活跃文件变为封存文件
		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		// 创建新的活跃文件
		activeFileId = activeFileId + 1
		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}
	// 写入entry数据，并更新offset
	if err := db.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}
	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset
	// 如果配置文件设置为sync，则需要将文件刷盘
	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
	}
	return nil
}
