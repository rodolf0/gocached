package expiry

//This is the node for the expiry heap. We just store the absolute expiry time and a pointer to the element key.
//Note that an entry on the heap does not guarantee that there's an entry on the storage map or that the map entry actually has expired. 
//The invariant is:
// Each entry in the storage map has at least one entry with the same key and ttl in the queue, or it's in the expiry channel buffer. This holds for the 'current' entry value at any given time, including updates

type Entry struct {
	Key     *string
	Exptime uint32
}

// The heap is implemented in an array, hence the type is an alias of Entry slice.
// Note that as we want to support extensible heap capacities, functions like Push and Pop may change the underlying array (Push), or re-slice (Pop). In either case, we need a slice pointer, to be able to update it as a side-effect.
type Heap []Entry

//make a heap of start_size and return its pointer
func NewHeap(start_size int) *Heap {
	r := make(Heap, 0, start_size)
	return &r
}

//heap.Interface Lean() implementation
func (h Heap) Len() int {
	return len(h)
}

//heap.Interface Less() implementation 
// here we could also take into consideration the key as a tie resolver, but it's not significant right now, as we don't care which of two entries with same exptime we want upper in the heap
func (h Heap) Less(i, j int) bool {
	return h[i].Exptime <= h[j].Exptime
}

// heap.Interface Swap implementation
func (h Heap) Swap(i, j int) {
	h[i].Exptime, h[j].Exptime = h[j].Exptime, h[i].Exptime
	h[i].Key, h[j].Key = h[j].Key, h[i].Key
}

// heap.Interface Push implementation. Semantic is 'append'. 
// Note that Push and Pop are implemented upon heap pointers, where Swap, Less and Len don't need pointers and are implemented on values. Recall 'the method set of the pointer type includes the method set of the value type'. May change the underlying array (and the slice)
func (h *Heap) Push(ientry interface{}) {
	entry := ientry.(Entry)
	*h = append(*h, entry)
}

// heap.Interface Pop implementation. Removes the last element on the heap-array. Changes the slice
func (h *Heap) Pop() interface{} {
	last := len(*h) - 1
	ret := (*h)[last]
	*h = (*h)[:last]
	return ret
}

func (h Heap) Tip() Entry {
  return h[0]
}

