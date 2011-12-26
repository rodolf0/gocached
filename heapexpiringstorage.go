package main

import (
	"expiry"
	"time"
	"container/heap"
	"os"
)

//Implements a Storage interface with entry expiration.
//Expiration is based on notification of new/updated exptime values from storage to an expiring registry (heap).
//Basically we 'type embed' MapStorage, reimplementing the methods that needs to notify heap of new exptimes. Notification is done via channel 'bg'.
type NotifyStorage struct {
	MapStorage
	bg   chan expiry.Entry
	heap *expiry.Heap
}

//Daemon that waits on two events. One triggers expired item recollection from the storage (timer). The other (bg) receives updates on exptime from storage.
func (ns *NotifyStorage) ExpiringDaemon(freq int64) {
	logger.Println("Collecting")
	for timer := time.NewTicker(freq * 1000000);; {
		select {
		case <-timer.C:
			ns.Collect()
		case entry := <-ns.bg:
			ns.Update(entry)
		}
	}
	logger.Println("Exit Expiring Daemon")
}

//Update. Given an exptime update, stores the entry in a exptime ordered heap
func (ns *NotifyStorage) Update(entry expiry.Entry) {
	now := uint32(time.Seconds())
	if entry.Exptime > now {
		heap.Push(ns.heap, entry)
	}
}

// Inspects the exptime heap for candidates for expiration, and dispatches to storage.MaybeExpire. The heap won't contain any expired entry refs(*) when it exits
func (ns *NotifyStorage) Collect() {
	now := uint32(time.Seconds())
	h := ns.heap
	if h.Len() == 0 {
		return
	}
	logger.Printf("heap size: %v. heap: %v", h.Len(), *h)
	for h.Len() > 0 {
		tip := h.Tip()
		if tip.Exptime > now {
			break
		}
		h.Pop()
		logger.Println("trying to expire %+v at %v", tip, now)
		ns.MaybeExpire(*tip.Key, now)
	}
}
func newNotifyStorage(expiring_frequency int64) *NotifyStorage {
  ns := &NotifyStorage{}
  ns.Init(expiring_frequency)
  return ns
}
//Init an allocated NotifyStorage
func (ns *NotifyStorage) Init(daemon_freq int64) {
	logger.Println("init notify storage")
	ns.MapStorage.Init()
	ns.bg = make(chan expiry.Entry, 100)
	ns.heap = expiry.NewHeap(100)
	go ns.ExpiringDaemon(daemon_freq)
}

//Method Overrides for operations that may change or add a new expiration time for an entry

func (self *NotifyStorage) Set(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Set(key, flags, exptime, bytes, content)
	if err == nil {
		self.bg <- expiry.Entry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Add(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Add(key, flags, exptime, bytes, content)
	if err == nil {
		self.bg <- expiry.Entry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Replace(key string, flags uint32, exptime uint32, bytes uint32, content []byte) (err os.Error) {
	err = self.MapStorage.Replace(key, flags, exptime, bytes, content)
	if err == nil {
		self.bg <- expiry.Entry{&key, exptime}
	}
	return err
}

func (self *NotifyStorage) Cas(key string, flags uint32, exptime uint32, bytes uint32, cas_unique uint64, content []byte) (err os.Error) {
	err = self.MapStorage.Cas(key, flags, exptime, bytes, cas_unique, content)
	if err == nil {
		self.bg <- expiry.Entry{&key, exptime}
	}
	return err
}

// Expire item implementation for MapStorage. May go into Storage interface some day 
func (self *MapStorage) MaybeExpire(key string, now uint32) bool {
	self.rwLock.RLock()
	entry, present := self.storageMap[key]
	self.rwLock.RUnlock()
	if present && entry.exptime <= now {
		logger.Printf("expiring key %v %+v at %v", key, entry, now)
		self.rwLock.Lock()
		self.storageMap[key] = &mapStorageEntry{}, false
		self.rwLock.Unlock()
		return true
	} else {
		logger.Println("not expiring %v", key)
		return false
	}
	return false
}
