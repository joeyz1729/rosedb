package rosedb

import "zouyi/minidb/storage"

type DataIndexMode int

const (
	KeyValueMemMode DataIndexMode = iota
	KeyOnlyMemMode
)

const (
	DefaultAddr = "127.0.0.1:5200"

	DefaultDirPath = "/tmp/rosedb_server"

	DefaultBlockSize = 16 * 1024 * 1024

	DefaultMaxKeySize = uint32(128)

	DefaultMaxValueSize = uint32(1 * 1024 * 1024)

	DefaultReclaimThreshold = 4

	DefaultSingleReclaimThreshold = 4 * 1024 * 1024
)

type Config struct {
	Addr                   string               `json:"addr" toml:"addr"`             // server address
	DirPath                string               `json:"dir_path" toml:"dir_path"`     // rosedb dir path of db file
	BlockSize              int64                `json:"block_size" toml:"block_size"` // each db file size
	RwMethod               storage.FileRWMethod `json:"rw_method" toml:"rw_method"`   // db file read and write method
	IdxMode                DataIndexMode        `json:"idx_mode" toml:"idx_mode"`     // data index mode
	MaxKeySize             uint32               `json:"max_key_size" toml:"max_key_size"`
	MaxValueSize           uint32               `json:"max_value_size" toml:"max_value_size"`
	Sync                   bool                 `json:"sync" toml:"sync"`                           // sync to disk if necessary
	ReclaimThreshold       int                  `json:"reclaim_threshold" toml:"reclaim_threshold"` // threshold to reclaim disk
	SingleReclaimThreshold int64                `json:"single_reclaim_threshold"`                   // single reclaim threshold
}

func DefaultConfig() Config {
	return Config{
		Addr:                   DefaultAddr,
		DirPath:                DefaultDirPath,
		BlockSize:              DefaultBlockSize,
		RwMethod:               storage.FileID,
		IdxMode:                KeyValueMemMode,
		MaxKeySize:             DefaultMaxKeySize,
		MaxValueSize:           DefaultMaxValueSize,
		Sync:                   false,
		ReclaimThreshold:       DefaultReclaimThreshold,
		SingleReclaimThreshold: DefaultSingleReclaimThreshold,
	}
}
