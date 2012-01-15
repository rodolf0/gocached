package main

import (
  "sync"
  "time"
  "strconv"
)

type MapCacheStorage struct {
	storageMap map[string]*StorageEntry
	rwLock     sync.RWMutex
}

func newMapCacheStorage() *MapCacheStorage {
  storage := &MapCacheStorage{}
  storage.Init()
  return storage
}

func (self *MapCacheStorage) Init() {
	self.storageMap = make(map[string]*StorageEntry)
}

func (self *StorageEntry) expired() bool {
  if self.exptime == 0 {
    return false
  }
	now := uint32(time.Seconds())
  return self.exptime <= now
}

func (self *MapCacheStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (previous *StorageEntry, result *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	var newEntry *StorageEntry
	if present && !entry.expired() {
		newEntry = &StorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
	  self.storageMap[key] = newEntry
    return entry, newEntry
	}
	newEntry = &StorageEntry{exptime, flags, bytes, 0, content}
	self.storageMap[key] = newEntry
	return nil, newEntry
}

func (self *MapCacheStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err ErrorCode, result *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return KeyAlreadyInUse, nil
	}
  entry = &StorageEntry{exptime, flags, bytes, 0, content}
	self.storageMap[key] = entry
	return Ok, entry
}

func (self *MapCacheStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (ErrorCode,*StorageEntry,*StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newEntry := &StorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Append(key string, bytes uint32, content []byte) (ErrorCode,*StorageEntry,*StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, entry.content)
		copy(newContent[len(entry.content):], content)
		newEntry := &StorageEntry{entry.exptime, entry.flags, bytes + entry.bytes, entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Prepend(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		newContent := make([]byte, len(entry.content)+len(content))
		copy(newContent, content)
		copy(newContent[len(content):], entry.content)
		newEntry := &StorageEntry{entry.exptime, entry.flags, bytes + entry.bytes,
			entry.cas_unique + 1, newContent}
		self.storageMap[key] = newEntry
		return Ok, entry, newEntry
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		if entry.cas_unique == cas_unique {
			newEntry := &StorageEntry{exptime, flags, bytes, cas_unique, content}
			self.storageMap[key] = newEntry
			return Ok, entry, newEntry
		} else {
			return IllegalParameter, entry, nil
		}
	}
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Get(key string) (ErrorCode, *StorageEntry) {
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		return Ok, entry
	}
  return KeyNotFound, nil
}

func (self *MapCacheStorage) Delete(key string) (ErrorCode, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
	if present && !entry.expired() {
		self.storageMap[key] = nil, false
		return Ok, entry
	}
	return KeyNotFound, nil
}

func (self *MapCacheStorage) Incr(key string, value uint64, incr bool) (ErrorCode, *StorageEntry, *StorageEntry) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
	entry, present := self.storageMap[key]
  if present && !entry.expired() {
	  if addValue, err := strconv.Atoui(string(entry.content)); err == nil {
		  var incrValue uint64
		  if incr {
			  incrValue = uint64(addValue) + value
		  } else {
			  incrValue = uint64(addValue) - value
		  }
		  incrStrValue := strconv.Uitoa64(incrValue)
      old_value := entry.content
		  entry.content = []byte(incrStrValue)
		  return Ok, &StorageEntry{entry.exptime, entry.flags, entry.bytes, entry.cas_unique, old_value}, entry
	  } else {
	    return IllegalParameter, nil, nil
    }
  }
	return KeyNotFound, nil, nil
}

func (self *MapCacheStorage) Expire(key string) {
	self.rwLock.Lock()
	defer self.rwLock.Unlock()
  _, present := self.storageMap[key]
	if present {
		self.storageMap[key] = &StorageEntry{}, false
	}
}
