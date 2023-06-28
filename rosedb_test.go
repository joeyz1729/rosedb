package rosedb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const (
	keyPrefix   = "test_key_"
	valuePrefix = "test_value_"
)

func TestOpen(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestMiniDB_Put(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valuePrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, val)
	}

	if err != nil {
		t.Log(err)
	}
}

func TestMiniDB_Get(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}
	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {

			t.Error("read val error: ", err)

		} else {
			t.Logf("key = %s, val = %s", string(key), string(val))
		}

	}
	for i := 0; i < 5; i++ {
		getVal([]byte(keyPrefix + strconv.Itoa(i)))
	}

}

func TestMiniDB_Del(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_78")
	err = db.Del(key)

	if err != nil {
		t.Error("del err:", err)
	}

}

func TestMiniDB_Merge(t *testing.T) {
	db, err := Open("/tmp/minidb")
	if err != nil {
		t.Error(err)
	}

	err = db.Merge()
	if err != nil {
		t.Error("merge err:", err)
	}
}
