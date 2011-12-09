package main

import (
  "os"
)

type Hasher func (string) uint32

type HashingStorage struct {
  size uint32
  hasher Hasher
  storageBuckets []Storage
}

func newHashingStorage(size uint32) *HashingStorage {
  s := &HashingStorage{size, hornerHasher, make([]Storage, size)}
   
  for i := uint32(0); i < size; i++  {
    s.storageBuckets[i] = newMapStorage()
  }
  return s
}

func (self *HashingStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) { 
  return self.findBucket(key).Set(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
  return self.findBucket(key).Add(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
  return self.findBucket(key).Replace(key, flags, exptime, bytes, content)
}

func (self *HashingStorage) Append(key string, bytes uint32, content []byte) (err os.Error) {
  return self.findBucket(key).Append(key, bytes, content)
}

func (self *HashingStorage) Prepend(key string, bytes uint32, content []byte) (err os.Error) {
  return self.findBucket(key).Prepend(key, bytes, content)
}

func (self *HashingStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
  return self.findBucket(key).Cas(key, flags, exptime, bytes, cas_unique, content)
}

func (self *HashingStorage) Delete(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
  return self.findBucket(key).Delete(key)
}

func (self *HashingStorage) Incr(key string, value uint64, incr bool) (resultValue uint64, err os.Error) {
  return self.findBucket(key).Incr(key, value, incr)
}

func (self *HashingStorage) Get(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
  return self.findBucket(key).Get(key)
}

func (self *HashingStorage) findBucket(key string) Storage {
  return self.storageBuckets[self.hasher(key) % self.size]
}

var hornerHasher = func(value string) uint32 {
  var hashcode uint32 = 1
  for i := 0; i < len(value); i++ {
    hashcode += (hashcode * 31) + uint32(value[i])
  }
  return hashcode
}
