package main

import (
  "os"
  "io"
  "bufio"
  "strings"
  "fmt"
)



type CommandReader struct {
  rd *bufio.Reader
  state int
}

func NewCommandReader(rd io.Reader) (*CommandReader, os.Error) {
  var cmd_reader = new(CommandReader)
  cmd_reader.rd = bufio.NewReader(rd)
  return cmd_reader, nil
}


type Command interface {
  Exec()
}

type StorageCommand struct {
  command string
  key string
  flags uint32
  exptime uint32
  bytes uint32
  cas_unique uint64
  noreply bool
}

type RetrievalCommand struct {
  command string
  key string
}


// TODO: contemplate the case when ReadLine can't fit a line into the buffer
func (cr *CommandReader) Read() (*Command, os.Error) {
  if line, _, err := cr.rd.ReadLine(); err != nil {
    return nil, err
  } else {
    var cmdline = strings.Split(string(line), " ")
    for _, str := range cmdline {
      if len(str) == 0 { continue }
      fmt.Printf("%s\n", str)
    }
  }
  return nil, nil
}
