package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// DBMeta save some meta info of db.
type DBMeta struct {
	ActiveWriteOff   map[uint16]int64 `json:"active_write_off"`
	ReclaimableSpace map[uint32]int64 `json:"reclaimable_space"`
}

// LoadMeta load db meta from file.
func LoadMeta(path string) (meta *DBMeta) {
	meta = &DBMeta{
		ActiveWriteOff:   make(map[uint16]int64),
		ReclaimableSpace: make(map[uint32]int64),
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, meta)
	return

}

// Store store db meta as json.
func (meta *DBMeta) Store(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	b, _ := json.Marshal(meta)
	_, err = file.Write(b)
	return err
}
