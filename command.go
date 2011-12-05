package main

import (
  "os"
  "net"
  "bufio"
  "strings"
  "strconv"
  "regexp"
)

var spaceMatcher, _ = regexp.Compile("  *")

type Session struct {
  conn      *net.TCPConn
  bufreader *bufio.Reader
}

type Command interface {
  Exec(s *Session) os.Error
}

/*type CommandError interface {*/
  /*String() string*/
  /*Reply(io.Writer)*/
/*}*/

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


func NewSession(conn *net.TCPConn) (*Session, os.Error) {
  var s = new(Session)
  s.conn = conn
  s.bufreader = bufio.NewReader(conn)
  return s, nil
}


func (s *Session) NextCommand() (Command, os.Error) {
  var line []string
  if rawline, _, err := s.bufreader.ReadLine(); err != nil {
    return nil, err
  } else {
    line = strings.Split(spaceMatcher.ReplaceAllString(string(rawline), " "), " ")
  }

  switch line[0] {
  case "set", "add", "replace", "append", "prepend", "cas":
    command := new(StorageCommand)
    if err := command.parse(line); err != nil {
      return nil, err
    } else if err := command.readData(s.bufreader); err != nil {
      return nil, err
    }
    return command, nil

  case "get", "gets":
    /*command = new(RetrieveCommand)*/

  case "delete":
  case "incr", "decr":
  case "touch":
  case "stats":
  case "flush_all":
  case "version":
  case "quit":
  }

  return nil, os.NewError("Unrecognized command: " + line[0])
}


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
    return os.NewError("CLIENT_ERROR: bad data chunk")
  }
  sc.data = sc.data[:len(sc.data)-2] // strip \n\r
  return nil
}


func (sc *StorageCommand) Exec(s *Session) os.Error {
  // TODO: call map storage functions
  logger.Printf("Storage: key: %s, flags: %d, exptime: %d, " +
                "bytes: %d, cas: %d, noreply: %t, content: %s\n",
                sc.key, sc.flags, sc.exptime, sc.bytes,
                sc.cas_unique, sc.noreply, string(sc.data))
  return nil
}
