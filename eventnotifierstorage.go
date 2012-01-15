package main

type EventNotifierStorage struct {
  updatesChannel chan UpdateMessage
  storage CacheStorage
}

type UpdateMessage struct {
  op int
  key string
  currentEpoch int64
  newEpoch int64
}

const (
  Delete = iota
  Add
  Change
  Collect
)

func updateMessageLogger(updatesChannel chan UpdateMessage) {
  for {
    m := <-updatesChannel
    logger.Printf("New message: op: %d, key: %s, currentEpoch: %d, newEpoch: %d", m.op, m.key, m.currentEpoch, m.newEpoch)
  }
}

func newEventNotifierStorage(storage CacheStorage, updatesChannel chan UpdateMessage) *EventNotifierStorage {
  return &EventNotifierStorage{updatesChannel, storage}
}

func (self *EventNotifierStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (*StorageEntry, *StorageEntry) {
  previous, updated := self.storage.Set(key, flags, exptime, bytes, content)
  if (previous != nil) {
    self.updatesChannel <- UpdateMessage{Change, key, int64(previous.exptime), int64(exptime)}
  } else {
    self.updatesChannel <- UpdateMessage{Add, key, 0, int64(exptime)}
  }
  return previous, updated
}

func (self *EventNotifierStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (ErrorCode, *StorageEntry) {
  err, updatedEntry := self.storage.Add(key, flags, exptime, bytes, content)
  if (err == Ok) {
    self.updatesChannel <- UpdateMessage{Add, key, 0, int64(exptime)}
  }
  return err, updatedEntry
}

func (self *EventNotifierStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  err, prev, updated := self.storage.Replace(key, flags, exptime, bytes, content)
  if (err == Ok) {
    self.updatesChannel <- UpdateMessage{Change, key, int64(prev.exptime), int64(exptime)}
  }
  return err, prev, updated
}

func (self *EventNotifierStorage) Append(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.storage.Append(key, bytes, content)
}

func (self *EventNotifierStorage) Prepend(key string, bytes uint32, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.storage.Prepend(key, bytes, content)
}

func (self *EventNotifierStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (ErrorCode, *StorageEntry, *StorageEntry) {
  err, prev, updated := self.storage.Cas(key, flags, exptime, bytes, cas_unique, content)
  if (err == Ok) {
    self.updatesChannel <- UpdateMessage{Change, key, int64(prev.exptime), int64(exptime)}
  }
  return err, prev, updated
}

func (self *EventNotifierStorage) Get(key string) (err ErrorCode, result *StorageEntry) {
  return self.storage.Get(key)
}

func (self *EventNotifierStorage) Delete(key string) (ErrorCode, *StorageEntry) {
  err, deleted := self.storage.Delete(key)
  if (err == Ok) {
    self.updatesChannel <- UpdateMessage{Delete, key, int64(deleted.exptime), 0}
  }
  return err, deleted
}

func (self *EventNotifierStorage) Incr(key string, value uint64, incr bool) (ErrorCode, *StorageEntry, *StorageEntry) {
  return self.storage.Incr(key, value, incr)
}

func (self *EventNotifierStorage) Expire(key string) {
  self.storage.Expire(key)
}
