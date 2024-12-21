// https://raw.githubusercontent.com/tylertreat/BoomFilters/1a82519a3e43a388296ba6cc389e1bc5dff5f7b8/topk.go

package boom

import (
	boom "github.com/tylertreat/BoomFilters"
)

// TopK uses a Count-Min Sketch to calculate the top-K frequent elements in a
// stream.
type TopK struct {
	cms *boom.CountMinSketch
	n   uint
	db  ElementsStorage
}

// NewTopK creates a new TopK backed by a Count-Min sketch whose relative
// accuracy is within a factor of epsilon with probability delta. It tracks the
// k-most frequent elements.
func NewTopK(epsilon, delta float64, k uint) *TopK {
	return &TopK{
		cms: boom.NewCountMinSketch(epsilon, delta),
		db:  New(k),
	}
}

// Add will add the data to the Count-Min Sketch and update the top-k heap if
// applicable. Returns the TopK to allow for chaining.
func (t *TopK) Add(data []byte) *TopK {
	t.cms.Add(data)
	t.n++

	freq := t.cms.Count(data)
	if t.db.isTop(freq) {
		t.db.Insert(data, freq)
	}

	return t
}

// Elements returns the top-k elements from lowest to highest frequency.
func (t *TopK) Elements() []*Element {
	return t.db.Elements()
}

// Reset restores the TopK to its original state. It returns itself to allow
// for chaining.
func (t *TopK) Reset() *TopK {
	t.cms.Reset()
	t.db.Reset()
	t.n = 0
	return t
}
