package main

import (
  "os"
  "sync"
  "strconv"
  "time"
)

type mapStorageEntry struct {
  exptime    uint32
  flags      uint32
  bytes      uint32
  cas_unique uint64
  content    []byte
}

type MapStorage struct {
	storageMap map[string]mapStorageEntry
	rwLock     sync.RWMutex
}

func newMapStorage() *MapStorage {
  storage := &MapStorage{}
  storage.Init()
  return storage
}

func (self *MapStorage) Init() {
	self.storageMap = make(map[string]mapStorageEntry)
}

func (self *mapStorageEntry) expired() bool {
  if self.exptime == 0 {
    return false
  }
	now := uint32(time.Seconds())
  return self.exptime <= now
}

func (self *MapStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	var newEntry mapStorageEntry
	if present {
		newEntry = mapStorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
	} else {
		newEntry = mapStorageEntry{exptime, flags, bytes, 0, content}
	}
	self.storageMap[key] = newEntry
	return nil
}

func (self *MapStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return os.NewError("Key already in use")
	}
	self.storageMap[key] = mapStorageEntry{exptime, flags, bytes, 0, content}
	return nil
}

func (self *MapStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present {
		newEntry := mapStorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
		self.storageMap[key] = newEntry
		return nil
	}
	return os.NewError("Key not found")
}

func (self *MapStorage) Append(key string, bytes uint32, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, entry.content)
		copy(newContent[len(entry.content):], content)
		newEntry := mapStorageEntry{entry.exptime, entry.flags, bytes + entry.bytes, entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return nil
	}
	return os.NewError("Key not found")
}

func (self *MapStorage) Prepend(key string, bytes uint32, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, content)
		copy(newContent[len(content):], entry.content)
		newEntry := mapStorageEntry{entry.exptime, entry.flags, bytes + entry.bytes,
			entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return nil
	}
	return os.NewError("Key not found")
}

func (self *MapStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present {
		if entry.cas_unique == cas_unique {
			newEntry := mapStorageEntry{exptime, flags, bytes, cas_unique, content}
			self.storageMap[key] = newEntry
			return nil
		} else {
			return os.NewError("Invalid cas value")
		}
	}
	return os.NewError("Key not found")
}

func (self *MapStorage) Delete(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present {
		self.storageMap[key] = mapStorageEntry{}, false
		return entry.flags, entry.bytes, entry.cas_unique, entry.content, nil
	}
	return 0, 0, 0, nil, os.NewError("Key not found")
}

func (self *MapStorage) Incr(key string, value uint64, incr bool) (resultValue uint64, err os.Error) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if !present {
		return 0, os.NewError("Key not found")
	}
	if addValue, err := strconv.Atoui(string(entry.content)); err == nil {
		var incrValue uint64
		if incr {
			incrValue = uint64(addValue) + value
		} else {
			incrValue = uint64(addValue) - value
		}
		incrStrValue := strconv.Uitoa64(incrValue)
		entry.content = []byte(incrStrValue)
		return incrValue, nil
	}
	return 0, os.NewError("Error: bad formed decimal value")
}

func (self *MapStorage) Get(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return entry.flags, entry.bytes, entry.cas_unique, entry.content, nil
	}
  logger.Printf("Expired entry %+v at %v", entry, time.Seconds())
  return 0, 0, 0, nil, os.NewError("Expired entry")
}
