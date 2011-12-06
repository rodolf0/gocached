package main

import (
  "os"
  "net"
  "bufio"
  "strings"
  "strconv"
  "regexp"
  "fmt"
)

var spaceMatcher, _ = regexp.Compile("  *")

type Session struct {
  conn      *net.TCPConn
  bufreader *bufio.Reader
  storage Storage
}

type Command interface {
  Exec(s *Session) os.Error
}

type StorageCommand struct {
  command     string
  key         string
  flags       uint32
  exptime     uint32
  bytes       uint32
  cas_unique  uint64
  noreply     bool
  data        []byte
}

type RetrievalCommand struct {
  command  string
  keys     []string
}

const (
  NA = iota
  UnkownCommand
  ClientError
  ServerError
)

type ErrCommand struct {
  errtype     int
  errdesc     string
  os_err      os.Error
}

func NewSession(conn *net.TCPConn) (*Session, os.Error) {
  var s = &Session{conn, bufio.NewReader(conn), newMapStorage()}
  return s, nil
}

func (s *Session) NextCommand() Command {
  var line []string
  if rawline, _, err := s.bufreader.ReadLine(); err != nil {
    return &ErrCommand{NA, "", err}
  } else {
    line = strings.Split(spaceMatcher.ReplaceAllString(string(rawline), " "), " ")
  }

  switch line[0] {
  case "set", "add", "replace", "append", "prepend", "cas":
    command := new(StorageCommand)
    if err := command.parse(line); err != nil {
      return &ErrCommand{ClientError, "bad command line format", err}
    } else if err := command.readData(s.bufreader); err != nil {
      return &ErrCommand{ClientError, "bad data chunk", err}
    }
    return command

  case "get", "gets":
    command := new(RetrievalCommand)
    if err := command.parse(line); err != nil {
      return &ErrCommand{ClientError, "bad command line format", err}
    }
    return command
  case "delete":
  case "incr", "decr":
  case "touch":
  case "stats":
  case "flush_all":
  case "version":
  case "quit":
  }

  return &ErrCommand{UnkownCommand, "",
                     os.NewError("Unkown command: " + line[0])}
}

////////////////////////////// ERROR COMMANDS //////////////////////////////

func (e *ErrCommand) Exec(s *Session) os.Error {
  var msg string
  switch e.errtype {
  case UnkownCommand: msg = "ERROR\r\n"
  case ClientError: msg = "CLIENT_ERROR " + e.errdesc + "\r\n"
  case ServerError: msg = "SERVER_ERROR " + e.errdesc + "\r\n"
  }
  if e.os_err != nil {
    logger.Println(e.os_err)
  }
  if _, err := s.conn.Write([]byte(msg)); err != nil {
    return err
  }
  return nil
}

///////////////////////////// RETRIEVAL COMMANDS ////////////////////////////


func (sc *RetrievalCommand) parse(line []string) os.Error {
  if len(line) < 2 {
    return os.NewError("Bad retrieval command: missing parameters")
  }
  sc.command = line[0]
  sc.keys = line[1:]
  return nil
}

func (self *RetrievalCommand) Exec(s *Session) os.Error {
  
  logger.Printf("Retrieval: command: %s, keys: %s", self.command, self.keys)
  showAll := self.command == "gets"
  for i := 0; i < len(self.keys); i++ {
    if flags, bytes, cas_unique, content, err := s.storage.Get(self.keys[i]); err != nil {
      //Internal error. Just close the connection
      s.conn.Close()
    } else {
      if showAll {
        s.conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", self.keys[i], flags, bytes, cas_unique)))
      } else {
        s.conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d\r\n", self.keys[i], flags, bytes)))
      }
      s.conn.Write(content)
      s.conn.Write([]byte("\r\n"))   
    }
  }
  return nil
} 

///////////////////////////// STORAGE COMMANDS /////////////////////////////

func (sc *StorageCommand) parse(line []string) os.Error {
  var flags, exptime, bytes, casuniq uint64
  var err os.Error
  if len(line) < 5 {
    return os.NewError("Bad storage command: missing parameters")
  } else if flags, err = strconv.Atoui64(line[2]); err != nil {
    return os.NewError("Bad storage command: bad flags")
  } else if exptime, err = strconv.Atoui64(line[3]); err != nil {
    return os.NewError("Bad storage command: bad expiration time")
  } else if bytes, err = strconv.Atoui64(line[4]); err != nil {
    return os.NewError("Bad storage command: bad expiration time")
  } else if line[0] == "cas" {
    if casuniq, err = strconv.Atoui64(line[5]); err != nil {
      return os.NewError("Bad storage command: bad cas value")
    }
  }
  sc.command = line[0]
  sc.key = line[1]
  sc.flags = uint32(flags)
  sc.exptime = uint32(exptime)
  sc.bytes = uint32(bytes)
  sc.cas_unique = casuniq
  if line[len(line)-1] == "noreply" {
    sc.noreply = true
  }
  return nil
}


func (sc *StorageCommand) readData(rd *bufio.Reader) os.Error {
  if sc.bytes <= 0 {
    return os.NewError("Bad storage operation: trying to read 0 bytes")
  } else {
    sc.data = make([]byte, sc.bytes + 2) // \r\n is always present at the end
  }
  // read all the data
  for offset := 0; offset < int(sc.bytes); {
    if nread, err := rd.Read(sc.data[offset:]); err != nil {
      return err
    } else {
      offset += nread
    }
  }
  if string(sc.data[len(sc.data)-2:]) != "\r\n" {
    return os.NewError("Bad storage operation: bad data chunk")
  }
  sc.data = sc.data[:len(sc.data)-2] // strip \n\r
  return nil
}


func (sc *StorageCommand) Exec(s *Session) os.Error {
  logger.Printf("Storage: key: %s, flags: %d, exptime: %d, " +
                "bytes: %d, cas: %d, noreply: %t, content: %s\n",
                sc.key, sc.flags, sc.exptime, sc.bytes,
                sc.cas_unique, sc.noreply, string(sc.data))
  
  switch(sc.command) {

  case "set":
    if err := s.storage.Set(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data) ; err != nil {
      //This is an internal error. Connection should be closed by server.
      s.conn.Close() 
    } else if !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
    return nil
  case "add":
    if err := s.storage.Add(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data) ; err != nil && !sc.noreply {
      s.conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
  case "replace": 
    if err := s.storage.Replace(sc.key, sc.flags, sc.exptime, sc.bytes, sc.data) ; err != nil && !sc.noreply {
      s.conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
  case "append":
    if err := s.storage.Append(sc.key, sc.bytes, sc.data) ; err != nil && !sc.noreply {
      s.conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
  case "prepend":
    if err := s.storage.Prepend(sc.key, sc.bytes, sc.data) ; err != nil && !sc.noreply {
      s.conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
  case "cas":
    if err := s.storage.Cas(sc.key, sc.flags, sc.exptime, sc.bytes, sc.cas_unique, sc.data) ; err != nil && !sc.noreply {
      //Fix this. We need special treatment for "exists" and "not found" error.
      s.conn.Write([]byte("EXISTS\r\n"))
    } else if err == nil && !sc.noreply {
      s.conn.Write([]byte("STORED\r\n"))
    }
  }
  return nil
}
