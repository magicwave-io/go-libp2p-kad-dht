package boom

import (
	"bytes"
	"container/heap"
	"time"
)

// Element represents a data and it's frequency
type Element struct {
	Data        []byte
	Freq        uint64
	LastUpdated time.Time
}

// An elementHeap is a min-heap of elements.
type elementHeap []*Element

func (e elementHeap) Len() int           { return len(e) }
func (e elementHeap) Less(i, j int) bool { return e[i].Freq < e[j].Freq }
func (e elementHeap) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

func (e *elementHeap) Push(x interface{}) {
	*e = append(*e, x.(*Element))
}

func (e *elementHeap) Pop() interface{} {
	old := *e
	n := len(old)
	x := old[n-1]
	*e = old[0 : n-1]
	return x
}

type ElementsStorage interface {
	Elements() []*Element
	Reset()
	Insert(data []byte, freq uint64)
	isTop(freq uint64) bool
}

func New(k uint) ElementsStorage {
	elements := make(elementHeap, 0, k)
	heap.Init(&elements)

	return &elementsStorage{
		k:        k,
		elements: &elements,
	}
}

type elementsStorage struct {
	k uint

	elements *elementHeap
}

func (t *elementsStorage) Elements() []*Element {
	if t.elements.Len() == 0 {
		return make([]*Element, 0)
	}

	elements := make(elementHeap, t.elements.Len())
	copy(elements, *t.elements)
	heap.Init(&elements)
	topK := make([]*Element, 0, t.k)

	for elements.Len() > 0 {
		topK = append(topK, heap.Pop(&elements).(*Element))
	}

	return topK
}

func (es *elementsStorage) Remove(i int) {
	heap.Remove(es.elements, i)
}
func (es *elementsStorage) Pop() {
	heap.Pop(es.elements)
}
func (es *elementsStorage) Push(e *Element) {
	heap.Push(es.elements, e)
}

func (es *elementsStorage) Reset() {
	elements := make(elementHeap, 0, es.k)
	heap.Init(&elements)
	es.elements = &elements
}

func (es *elementsStorage) isTop(freq uint64) bool {
	if es.elements.Len() < int(es.k) {
		return true
	}

	return freq >= (*es.elements)[0].Freq
}

// insert adds the data to the top-k heap. If the data is already an element,
// the frequency is updated. If the heap already has k elements, the element
// with the minimum frequency is removed.
func (es *elementsStorage) Insert(data []byte, freq uint64) {
	for i, element := range *es.elements {
		if bytes.Equal(data, element.Data) {
			// Element already in top-k, replace it with new frequency.
			es.Remove(i)
			element.Freq = freq
			element.LastUpdated = time.Now()
			es.Push(element)
			return
		}
	}

	if es.elements.Len() == int(es.k) {
		// Remove minimum-frequency element.
		es.Pop()
	}

	// Add element to top-k.
	es.Push(&Element{
		Data:        data,
		Freq:        freq,
		LastUpdated: time.Now(),
	})
}
