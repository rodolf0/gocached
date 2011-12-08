package expiry

type Entry struct {
	Exptime uint32
	Key     *string
}

type Heap []Entry

func NewHeap(start_size int) *Heap {
	r := make(Heap, 0, start_size)
  return &r
}

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
  return h[i].Exptime < h[j].Exptime
}

func (h Heap) Swap(i, j int) {
	h[i].Exptime, h[j].Exptime = h[j].Exptime, h[i].Exptime
	h[i].Key, h[j].Key = h[j].Key, h[i].Key
}

func (h *Heap) Push(ientry interface{}) {
	entry := ientry.(Entry)
	*h = append(*h, entry)
}

func (h *Heap) Pop() interface{} {
	last := len(*h) - 1
	ret := (*h)[last]
	*h = (*h)[:last]
	return ret
}

