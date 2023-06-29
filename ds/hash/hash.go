package hash

type (
	Hash struct {
		// Hash table struct.
		record Record
	}

	// Record hash records to save, key-field-value
	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{make(Record)}
}

// HSet create or overwritten, key-field-value
func (h *Hash) HSet(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if h.record[key][field] != nil {
		// overwritten
		h.record[key][field] = value
	} else {
		// create
		h.record[key][field] = value
		res = 1
	}
	return
}

// HSetNx create but not overwritten , key-field-value
func (h *Hash) HSetNx(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return 1
	}
	return 0
}

// HGet returns value of key-field
func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}
	return h.record[key][field]
}

// HGetAll returns all fields and values of hash stored at key.
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}
	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

// HDel removes the specified fields from hash stored at key.
func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}

	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

// HKeyExists returns if key exists in hash.
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// HExists returns if field is an existing field in the hash stored at key.
func (h *Hash) HExists(key, field string) (res int) {
	if !h.exist(key) {
		return
	}

	if _, exist := h.record[key][field]; exist {
		res = 1
	}
	return
}

// HLen returns the number of fields contained in the hash stored at key.
func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

// HKeys returns all field names in the hash stored at key.
func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}
	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

// HValues returns all values in the hash stored at key.
func (h *Hash) HValues(key string) (val [][]byte) {
	if !h.exist(key) {
		return
	}
	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

// HClear the key in hash
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
