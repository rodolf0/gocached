package expiry

import (
	"container/heap"
  "testing"
)


func TestPushPopNoExpand(t *testing.T) {
  h := NewHeap(3)
  heap.Push(h, Entry{10,nil})
  heap.Push(h, Entry{1,nil})
  heap.Push(h, Entry{5,nil})

  for _, expected := range []uint32{1,5,10} {
    v := heap.Pop(h).(Entry).Exptime
    if v != expected {
      t.Error("Not in order, expected", expected, "got", v)
    }
  }
}

func TestPushPopExpand(t *testing.T) {
  h := NewHeap(3)
  heap.Push(h, Entry{10,nil})
  heap.Push(h, Entry{1,nil})
  heap.Push(h, Entry{5,nil})
  heap.Push(h, Entry{50,nil})
  heap.Push(h, Entry{72,nil})
  heap.Push(h, Entry{17,nil})

  for _, expected := range []uint32{1,5,10,17,50,72} {
    v := heap.Pop(h).(Entry).Exptime
    if v != expected {
      t.Error("Not in order, expected", expected, "got", v)
    }
  }
}
