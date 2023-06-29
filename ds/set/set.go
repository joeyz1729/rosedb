package set

var existFlag = struct{}{}

type (
	Set struct {
		record Record
	}

	// Record in set to save.
	Record map[string]map[string]struct{}
)

func New() *Set {
	return &Set{make(Record)}
}

func (s *Set) SAdd(key string, member []byte) int {
	if !s.exist(key) {
		s.record[key] = make(map[string]struct{})
	}

	s.record[key][string(member)] = existFlag
	return len(s.record[key])
}

// SPop pop random members from specified set.
func (s *Set) SPop(key string, count int) (val [][]byte) {
	if !s.exist(key) || count <= 0 {
		return
	}

	for k := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))

		count--
		if count == 0 {
			break
		}
	}
	return
}

// SRandMember returns a random element from the set value stored at key
func (s *Set) SRandMember(key string, count int) (val [][]byte) {
	if !s.exist(key) || count == 0 {
		return
	}

	if count > 0 {
		for k := range s.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return
}

// SRem Remove specified members from the set stored.
func (s *Set) SRem(key string, member []byte) bool {
	if !s.exist(key) {
		return false
	}

	if _, ok := s.record[key][string(member)]; ok {
		delete(s.record[key], string(member))
		return true
	}
	return false
}

// SMove move member from src set to dst set
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.fieldExist(src, string(member)) {
		return false
	}
	if !s.exist(dst) {
		s.record[dst] = make(map[string]struct{})
	}
	delete(s.record[src], string(member))
	s.record[dst][string(member)] = existFlag

	return true
}

func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}
	return len(s.record[key])
}

// SUnion returns all members of given sets.
func (s *Set) SUnion(keys ...string) (val [][]byte) {
	m := make(map[string]bool)
	for _, k := range keys {
		if s.exist(k) {
			for v := range s.record[k] {
				m[v] = true
			}
		}
	}

	for v := range m {
		val = append(val, []byte(v))
	}
	return
}

// SDiff returns the difference between the first set and all the successive set.
func (s *Set) SDiff(keys ...string) (val [][]byte) {
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}
	for v := range s.record[keys[0]] {
		flag := true
		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}
	for k := range s.record[key] {
		val = append(val, []byte(k))
	}
	return
}

func (s *Set) SIsMember(key string, member []byte) bool {
	return s.fieldExist(key, string(member))
}

func (s *Set) SKeyExists(key string) (ok bool) {
	return s.exist(key)
}

func (s *Set) SClear(key string) {
	if s.SKeyExists(key) {
		delete(s.record, key)
	}
}

func (s *Set) exist(key string) bool {
	_, exist := s.record[key]
	return exist
}

func (s *Set) fieldExist(key, field string) bool {
	fields, exist := s.record[key]
	if !exist {
		return false
	}
	_, ok := fields[field]
	return ok
}
