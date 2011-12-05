package main

import (
  "os"
  "io"
  "bufio"
  "strings"
  "fmt"
  "strconv"
)

var MainStorage Storage = newMapStorage()

type CommandReader struct {
  rd *bufio.Reader
}

func NewCommandReader(rd io.Reader) (*CommandReader, os.Error) {
  cmd_reader := &CommandReader{}
  cmd_reader.rd = bufio.NewReader(rd)
  return cmd_reader, nil
}

type Command interface {
  Exec() (os.Error)
}

type StorageCommand struct {
  command string
  key string
  flags uint32
  exptime uint32
  bytes uint32
  cas_unique uint64
  noreply bool
  content []byte
}


func (self *StorageCommand) parseLine(line string) {
    var params = strings.Split(line, " ")
    
    self.key = params[1]
    if flags, err := strconv.Atoui(params[2]); err == nil { 
      self.flags = uint32(flags)
    }
    if exptime, err := strconv.Atoui(params[3]); err == nil { 
      self.exptime = uint32(exptime)
    }
    if bytes, err := strconv.Atoui(params[4]); err == nil { 
      self.bytes = uint32(bytes)
    }
    if cas_unique, err := strconv.Atoui(params[5]); err == nil { 
      self.cas_unique = uint64(cas_unique)
    }
    if len(params) == 7 {
      if noreply, err := strconv.Atob(params[6]); err == nil { 
        self.noreply = noreply
      }
    } else {
      self.noreply = false
    }
}

func (self *StorageCommand) Exec() (err os.Error){
    fmt.Printf("Storage: key: %s, flags: %d, exptime: %d, bytes: %d, cas: %d, noreply: %t, content: %s\n", 
      self.key, self.flags, self.exptime, self.bytes, self.cas_unique, self.noreply, string(self.content))
    return nil
}

type RetrievalCommand struct {
  command string
  key string
}

// TODO: contemplate the case when ReadLine can't fit a line into the buffer
func (cr *CommandReader) Read() (Command, os.Error) {
  if line, _, err := cr.rd.ReadLine(); err != nil || line == nil {
    return nil, err
  } else {
    var strLine = string(line)
    var cmdline = strings.Split(strLine, " ")
    if len(cmdline) < 1 {
      return nil, os.NewError("Bad formed command")
    }
    switch cmdline[0] {
    case "set":
        storage := &StorageCommand{}
        storage.parseLine(strLine)
        storage.content = make([]byte, storage.bytes)
        io.ReadFull(cr.rd, storage.content)
        return storage, nil
    }
  }
  return nil, nil
}


