package main

type Hasher func (string) uint32

type HashingStorage struct {
  size uint32
  hasher Hasher
  storageBuckets []CacheStorage
}

type StorageFactory func () Storage

func newHashingStorage(size uint32, factory CacheStorageFactory) *HashingStorage {
  s := &HashingStorage{size, hornerHasher, make([]CacheStorage, size)}
  for i := uint32(0); i < size; i++  {
    s.storageBuckets[i] = factory()
  }
  return s
}

func (self *HashingStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (previous *StorageEntry, result *StorageEntry) {
  return self.findBucket(key).Set(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err ErrorCode, result *StorageEntry) {
  return self.findBucket(key).Add(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (ErrorCode,*StorageEntry,*StorageEntry) {
  return self.findBucket(key).Replace(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Append(key string, bytes uint32, content []byte) (ErrorCode,*StorageEntry,*StorageEntry) {
  return self.findBucket(key).Append(key, bytes, content)
}

func (self *HashingStorage) Prepend(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.findBucket(key).Prepend(key, bytes, content)
}

func (self *HashingStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.findBucket(key).Cas(key, flags, exptime, bytes, cas_unique, content)
}

func (self *HashingStorage) Get(key string) (ErrorCode, *StorageEntry) {
  return self.findBucket(key).Get(key)
}

func (self *HashingStorage) Delete(key string) (ErrorCode, *StorageEntry) {
  return self.findBucket(key).Delete(key)
}

func (self *HashingStorage) Incr(key string, value uint64, incr bool) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.findBucket(key).Incr(key, value, incr)
}

func (self *HashingStorage) Expire(key string) {
  self.findBucket(key).Expire(key)
}

func (self *HashingStorage) findBucket(key string) CacheStorage {
  storageIndex := self.hasher(key) % self.size
  storage := self.storageBuckets[storageIndex]
 // logger.Printf("Using storage %d", storageIndex)
  return storage
}

var hornerHasher = func(value string) uint32 {
  var hashcode uint32 = 1
  for i := 0; i < len(value); i++ {
    hashcode += (hashcode * 31) + uint32(value[i])
  }
  return hashcode
}
