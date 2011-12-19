package main

import (
  "os"
  "net"
  "bufio"
  "strings"
  "strconv"
  "regexp"
  "time"
  "fmt"
)

type Session struct {
  conn      *net.TCPConn
  bufreader *bufio.Reader
  storage Storage
}

type Command interface {
  parse(line []string) bool
  Exec()
}

type StorageCommand struct {
  session     *Session
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
  session     *Session
  command     string
  keys     []string
}

type DeleteCommand struct {
  session     *Session
  command     string
  key string
  noreply bool
}

type TouchCommand struct {
  session     *Session
  command     string
  key string
  exptime uint32
  noreply bool
}

const (
  NA = iota
  UnkownCommand
  ClientError
  ServerError
)

func NewSession(conn *net.TCPConn) (*Session, os.Error) {
  var s = &Session{conn, bufio.NewReader(conn), newHashingStorage(1)}
  return s, nil
}


/* match more than one space */
var spaceMatcher, _ = regexp.Compile("  *")

/* Read a line and tokenize it */
func getTokenizedLine(r *bufio.Reader) []string {
  if rawline, _, err := r.ReadLine(); err == nil {
    return strings.Split(spaceMatcher.ReplaceAllString(string(rawline), " "), " ")
  }
  return nil
}


func (s *Session) CommandLoop() {

  for line := getTokenizedLine(s.bufreader);
      line != nil; line = getTokenizedLine(s.bufreader) {

    switch line[0] {

    case "set", "add", "replace", "append", "prepend", "cas":
      if cmd := (&StorageCommand{session: s}); cmd.parse(line) {
        cmd.Exec()
      }
    case "get", "gets":
      if cmd := (&RetrievalCommand{session: s}); cmd.parse(line) {
        cmd.Exec()
      }
    case "delete":
      if cmd := (&DeleteCommand{session: s}); cmd.parse(line) {
        cmd.Exec()
      }
    case "touch":
      if cmd := (&TouchCommand{session: s}); cmd.parse(line) {
        cmd.Exec()
      }
    case "incr", "decr", "stats", "flush_all", "version", "quit":

    default:
      Error(s, UnkownCommand, "")
    }
  }
}

////////////////////////////// ERROR COMMANDS //////////////////////////////

/* a function to reply errors to client that always returns false */
func Error(s *Session, errtype int, errdesc string) bool {
  var msg string
  switch errtype {
  case UnkownCommand: msg = "ERROR\r\n"
  case ClientError:   msg = "CLIENT_ERROR " + errdesc + "\r\n"
  case ServerError:   msg = "SERVER_ERROR " + errdesc + "\r\n"
  }
  logger.Println(msg)
  s.conn.Write([]byte(msg))
  return false
}

///////////////////////////// TOUCH COMMAND //////////////////////////////

const secondsInMonth = 60*60*24*30

func (self *TouchCommand) parse(line []string) bool {
  var exptime uint64
  var err os.Error
  if len(line) < 3 {
    return Error(self.session, ClientError, "Bad touch command: missing parameters")
  } else if exptime, err = strconv.Atoui64(line[2]); err != nil {
    return Error(self.session, ClientError, "Bad touch command: bad expiration time")
  }
  self.command = line[0]
  self.key = line[1]
  if exptime < secondsInMonth {
    self.exptime = uint32(time.Seconds()) + uint32(exptime);
  } else {
    self.exptime = uint32(exptime)
  }
  if line[len(line)-1] == "noreply" {
    self.noreply = true
  }
  return true
}

func (self *TouchCommand) Exec() {
  logger.Printf("Touch: command: %s, key: %s, , exptime %d, noreply: %t",
                self.command, self.key, self.exptime, self.noreply)
}

///////////////////////////// DELETE COMMAND ////////////////////////////

func (self *DeleteCommand) parse(line []string) bool {
  if len(line) < 2 {
    return Error(self.session, ClientError, "Bad delete command: missing parameters")
  }
  self.command = line[0]
  self.key = line[1]
  if line[len(line)-1] == "noreply" {
    self.noreply = true
  }
  return true
}

func (self *DeleteCommand) Exec() {
  logger.Printf("Delete: command: %s, key: %s, noreply: %t",
                self.command, self.key, self.noreply)
  var storage = self.session.storage
  var conn = self.session.conn
  if _, _,_,_,err := storage.Delete(self.key) ; err != nil && !self.noreply {
    conn.Write([]byte("NOT_FOUND\r\n"))
  } else if (err == nil && !self.noreply) {
    conn.Write([]byte("DELETED\r\n"))
  }
}

///////////////////////////// RETRIEVAL COMMANDS ////////////////////////////

func (self *RetrievalCommand) parse(line []string) bool {
  if len(line) < 2 {
    return Error(self.session, ClientError, "Bad retrieval command: missing parameters")
  }
  self.command = line[0]
  self.keys = line[1:]
  return true
}

func (self *RetrievalCommand) Exec() {
  logger.Printf("Retrieval: command: %s, keys: %s",
                self.command, self.keys)
  var storage = self.session.storage
  var conn = self.session.conn
  showAll := self.command == "gets"
  for i := 0; i < len(self.keys); i++ {
    if flags, bytes, cas_unique, content, err := storage.Get(self.keys[i]); err == nil {
      if showAll {
        conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d %d\r\n", self.keys[i], flags, bytes, cas_unique)))
      } else {
        conn.Write([]byte(fmt.Sprintf("VALUE %s %d %d\r\n", self.keys[i], flags, bytes)))
      }
      conn.Write(content)
      conn.Write([]byte("\r\n"))
    }
  }
  conn.Write([]byte("END\r\n"))
}

///////////////////////////// STORAGE COMMANDS /////////////////////////////

/* parse a storage command parameters and read the related data
   returns a flag indicating sucesss */
func (self *StorageCommand) parse(line []string) bool {
  var flags, exptime, bytes, casuniq uint64
  var err os.Error
  if len(line) < 5 {
    return Error(self.session, ClientError, "Bad storage command: missing parameters")
  } else if flags, err = strconv.Atoui64(line[2]); err != nil {
    return Error(self.session, ClientError, "Bad storage command: bad flags")
  } else if exptime, err = strconv.Atoui64(line[3]); err != nil {
    return Error(self.session, ClientError, "Bad storage command: bad expiration time")
  } else if bytes, err = strconv.Atoui64(line[4]); err != nil {
    return Error(self.session, ClientError, "Bad storage command: bad byte-length")
  } else if line[0] == "cas" {
    if casuniq, err = strconv.Atoui64(line[5]); err != nil {
      return Error(self.session, ClientError, "Bad storage command: bad cas value")
    }
  }
  self.command = line[0]
  self.key = line[1]
  self.flags = uint32(flags)
  if exptime < secondsInMonth {
    self.exptime = uint32(time.Seconds()) + uint32(exptime);
  } else {
    self.exptime = uint32(exptime)
  }
  self.bytes = uint32(bytes)
  self.cas_unique = casuniq
  if line[len(line)-1] == "noreply" {
    self.noreply = true
  }
  return self.readData()
}


/* read the data for a storage command and return a flag indicating success */
func (self *StorageCommand) readData() bool {
  if self.bytes <= 0 {
    return Error(self.session, ClientError, "Bad storage operation: trying to read 0 bytes")
  } else {
    self.data = make([]byte, self.bytes + 2) // \r\n is always present at the end
  }
  var reader = self.session.bufreader
  // read all the data
  for offset := 0; offset < int(self.bytes); {
    if nread, err := reader.Read(self.data[offset:]); err != nil {
      return Error(self.session, ServerError, "Failed to read data")
    } else {
      offset += nread
    }
  }
  if string(self.data[len(self.data)-2:]) != "\r\n" {
    return Error(self.session, ClientError, "Bad storage operation: bad data chunk")
  }
  self.data = self.data[:len(self.data)-2] // strip \n\r
  return true
}


func (self *StorageCommand) Exec() {
  logger.Printf("Storage: key: %s, flags: %d, exptime: %d, " +
                "bytes: %d, cas: %d, noreply: %t, content: %s\n",
                self.key, self.flags, self.exptime, self.bytes,
                self.cas_unique, self.noreply, string(self.data))

  var storage = self.session.storage
  var conn = self.session.conn

  switch self.command {

  case "set":
    if err := storage.Set(self.key, self.flags, self.exptime, self.bytes, self.data) ; err != nil {
      // This is an internal error. Connection should be closed by the server.
      conn.Close()
    } else if !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
    return
  case "add":
    if err := storage.Add(self.key, self.flags, self.exptime, self.bytes, self.data); err != nil && !self.noreply {
      conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
  case "replace":
    if err := storage.Replace(self.key, self.flags, self.exptime, self.bytes, self.data) ; err != nil && !self.noreply {
      conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
  case "append":
    if err := storage.Append(self.key, self.bytes, self.data) ; err != nil && !self.noreply {
      conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
  case "prepend":
    if err := storage.Prepend(self.key, self.bytes, self.data) ; err != nil && !self.noreply {
      conn.Write([]byte("NOT_STORED\r\n"))
    } else if err == nil && !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
  case "cas":
    if err := storage.Cas(self.key, self.flags, self.exptime, self.bytes, self.cas_unique, self.data) ; err != nil && !self.noreply {
      //Fix this. We need special treatment for "exists" and "not found" errors.
      conn.Write([]byte("EXISTS\r\n"))
    } else if err == nil && !self.noreply {
      conn.Write([]byte("STORED\r\n"))
    }
  }
}
