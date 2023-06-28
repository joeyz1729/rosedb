package index

import "zouyi/rosedb/storage"

// Indexer the data index info, stored in skip list
type Indexer struct {
	Meta      *storage.Meta
	FileId    uint32
	EntrySize uint32
	Offset    int64
}
