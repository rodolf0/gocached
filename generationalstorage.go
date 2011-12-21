package main

import (
  "sync"
  "time"
  "os"
  "strconv"
  "fmt"
)

const (
  Delete = iota
  Add
  Change
  Collect
  GCDelay = 5
  GenerationSize = 5
  StorageThreshold = 500
)

type UpdateMessage struct {
  op            int
  key           string
  currentEpoch  int64
  newEpoch      int64
}

var timer = func(updatesChannel chan UpdateMessage) {
  for {
    time.Sleep(1e9 * GCDelay) // one second * GCDelay
    updatesChannel <- UpdateMessage{Collect, "", time.Seconds(), 0}
  }
}

func (self *UpdateMessage) getCurrentTimeSlot() int64 {
  return roundTime(self.currentEpoch)
}

func (self *UpdateMessage) getNewTimeSlot() int64 {
  return roundTime(self.newEpoch)
}

func roundTime(time int64) int64 {
  return time - (time % GenerationSize) + GenerationSize
}

type Generation struct {
  startEpoch    int64
  inhabitants   map[string] bool
}

func (self *Generation) String() string {
  r := fmt.Sprintf("Generation [%s-%s]", time.SecondsToUTC(self.startEpoch), time.SecondsToUTC(self.startEpoch + GenerationSize))
  for key,_ := range(self.inhabitants) {
    r += fmt.Sprintf("\n %s", key)
  }
  return r
}

func newGeneration(epoch int64) *Generation {
  return &Generation{epoch, make(map[string] bool)}
}

type GenerationalStorage struct {
  rwLock          *sync.RWMutex
  storageMap      map[string]*mapStorageEntry
  generations     map[int64] *Generation
  updatesChannel  chan UpdateMessage
  lastCollected   int64
  items           int64
}

func newGenerationalStorage() *GenerationalStorage {
  updatesChannel := make(chan UpdateMessage)
  storage := &GenerationalStorage{new(sync.RWMutex), make(map [string] *mapStorageEntry), make(map [int64] *Generation), updatesChannel, roundTime(time.Seconds()) - GenerationSize, 0}
  go timer(updatesChannel)
  go processNodeChanges(storage, updatesChannel)
  return storage;
}

func (self *GenerationalStorage) removeGenerationToCollect(now int64) *Generation {
//  logger.Printf("Searching for expired generations %s", time.SecondsToUTC(now))
  if now >= self.lastCollected + GenerationSize {
    gen := self.generations[now]
    self.generations[now] = nil, false
    self.lastCollected += GenerationSize
    logger.Printf("Updating last collected generation to %s . Generation %s", time.SecondsToUTC(self.lastCollected), gen)
    return gen
  }
//  logger.Printf("Not enough time since last collection.")
  return nil
}

func (self *GenerationalStorage) findGeneration(timeSlot int64, createIfNotExists bool) *Generation {
  generation := self.generations[timeSlot]
  if generation == nil && createIfNotExists {
    logger.Printf("Creating new generation %s", time.SecondsToUTC(timeSlot))
    generation = newGeneration(timeSlot)
    self.generations[timeSlot] = generation
  }
  logger.Printf("Returning generation %s", generation)
  return generation
}

func (self *Generation) addInhabitant(key string) {
  logger.Printf("Adding key %s to generation %s", key,  time.SecondsToUTC(self.startEpoch))
  self.inhabitants[key] = true
}

func processNodeChanges(storage *GenerationalStorage, channel <-chan UpdateMessage /*, ticker *time.Ticker*/) {
  for {
   /* var msg UpdateMessage
    select {
    case msg = <-channel:
    case i := <-ticker.C:
      msg = UpdateMessage{Collect, "", i, 0}
    }*/
    msg := <-channel
    switch msg.op {
    case Add:
      logger.Println("Processing Add message")
      timeSlot := msg.getNewTimeSlot() 
      generation := storage.findGeneration(timeSlot, true)
      generation.addInhabitant(msg.key)
    case Delete:
      logger.Println("Processing Delete message")
      timeSlot := msg.getCurrentTimeSlot()
      if generation := storage.findGeneration(timeSlot, false); generation != nil {
        generation.inhabitants[msg.key] = false, false
      }
    case Change:
      logger.Println("Processing Change message")
      timeSlot := msg.getCurrentTimeSlot()
      if generation := storage.findGeneration(timeSlot, false); generation != nil {
        generation.inhabitants[msg.key] = false, false
      }
      newTimeSlot := msg.getNewTimeSlot()
      generation := storage.findGeneration(newTimeSlot, true)
      generation.addInhabitant(msg.key)
    case Collect:
      logger.Println("Processing Collect message")
      for {
        generation := storage.removeGenerationToCollect(msg.getCurrentTimeSlot()- GenerationSize)
        if generation == nil {
          break
        }
        logger.Printf("Collecting generation %d", generation)
        for key , _ := range(generation.inhabitants) {
          storage.rwLock.Lock()
          logger.Printf("Collecting item with key %s", key)
          storage.storageMap[key] = nil, false
          storage.rwLock.Unlock()
        }
      }
      if storage.items >StorageThreshold {
        logger.Println("Memory pressure. Collecting not expiring items")

      }
      logger.Println("No more items to collect")
    }
  }
}

func (self *GenerationalStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    newEntry = &mapStorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
    self.updatesChannel <- UpdateMessage{Change, key, int64(entry.exptime), int64(exptime)}
  } else {
    newEntry = &mapStorageEntry{exptime, flags, bytes, 0, content}
    self.updatesChannel <- UpdateMessage{Add, key, 0, int64(exptime)}
  }
  self.storageMap[key] = newEntry
  self.items += 1
  return nil
}

func (self *GenerationalStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()

  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    return os.NewError("Key already in use")
  }
  newEntry = &mapStorageEntry{exptime, flags, bytes, 0, content}
  self.storageMap[key] = newEntry
  self.items += 1
  self.updatesChannel <- UpdateMessage{Add, key, 0, int64(exptime)}
  return nil
}

func (self *GenerationalStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    newEntry = &mapStorageEntry{exptime, flags, bytes, entry.cas_unique + 1, content}
    self.storageMap[key] = newEntry
    self.updatesChannel <- UpdateMessage{Change, key, int64(entry.exptime), int64(exptime)}
    return nil
  }
  return os.NewError("Key not found")
}

func (self *GenerationalStorage) Append(key string, bytes uint32, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    newContent := make([]byte, len(entry.content)+len(content))
    copy(newContent, entry.content)
    copy(newContent[len(entry.content):], content)
    newEntry = &mapStorageEntry{entry.exptime, entry.flags, bytes + entry.bytes, entry.cas_unique + 1, newContent}
    self.storageMap[key] = newEntry
    return nil
  }
  return os.NewError("Key not found")
}

func (self *GenerationalStorage) Prepend(key string, bytes uint32, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    newContent := make([]byte, len(entry.content)+len(content))
    copy(newContent, content)
    copy(newContent[len(content):], entry.content)
    newEntry = &mapStorageEntry{entry.exptime, entry.flags, bytes + entry.bytes,
    entry.cas_unique + 1, newContent}
    self.storageMap[key] = newEntry
    return nil
  }
  return os.NewError("Key not found")
}

func (self *GenerationalStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  var newEntry *mapStorageEntry
  if entry != nil {
    if entry.cas_unique == cas_unique {
      newEntry = &mapStorageEntry{ exptime, flags, bytes, cas_unique, content }
      self.storageMap[key] = newEntry
      self.updatesChannel <- UpdateMessage{Change, key, int64(entry.exptime), int64(exptime)}
      return nil
    } else {
      return os.NewError("Invalid cas value")
    }
  }
  return os.NewError("Key not found")
}

func (self *GenerationalStorage) Delete(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  if entry == nil {
    return 0, 0, 0, nil, os.NewError("Key not found")
  }
  self.storageMap[key] = nil, false
  self.updatesChannel <- UpdateMessage{Delete, key, int64(entry.exptime), 0}
  self.items -= 1
  return entry.flags, entry.bytes, entry.cas_unique, entry.content, nil
}


func (self *GenerationalStorage) Get(key string) (flags uint32, bytes uint32, cas_unique uint64, content []byte, err os.Error) {
  self.rwLock.RLock()
  defer self.rwLock.RUnlock()
  entry := self.storageMap[key]
  if entry == nil {
    return 0, 0, 0, nil, os.NewError("Key not found")
  }
  return entry.flags, entry.bytes, entry.cas_unique, entry.content, nil
}

func (self *GenerationalStorage) Incr(key string, value uint64, incr bool) (resultValue uint64, err os.Error) {
  self.rwLock.Lock()
  defer self.rwLock.Unlock()
  entry := self.storageMap[key]
  if entry == nil {
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
