package rosedb

import (
	"sync"
)

type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}
