package kpeerset

import (
	"container/heap"
	"sort"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

type SortablePeers interface {
	sort.Interface
	GetPeerID(i int) peer.ID
}

func NewSortedPeerset(kvalue int, from string, sortPeers func([]IPeerMetric) SortablePeers) *SortedPeerset {
	fromKey := ks.XORKeySpace.Key([]byte(from))

	return &SortedPeerset{
		kvalue:          kvalue,
		from:            fromKey,
		heapTopKPeers:   peerMetricHeap{direction: 1},
		heapRestOfPeers: peerMetricHeap{direction: -1},
		topKPeers:       make(map[peer.ID]*peerMetricHeapItem),
		restOfPeers:     make(map[peer.ID]*peerMetricHeapItem),
		queried:         make(map[peer.ID]struct{}),
		sortPeers:       sortPeers,
	}
}

type SortedPeerset struct {
	kvalue int

	// from is the Key this PQ measures against
	from ks.Key

	heapTopKPeers, heapRestOfPeers peerMetricHeap

	topKPeers, restOfPeers map[peer.ID]*peerMetricHeapItem
	queried                map[peer.ID]struct{}

	sortPeers func([]IPeerMetric) SortablePeers

	lock sync.Mutex
}

// Add adds the peer to the set. It returns true if the peer was newly added to the topK peers.
func (ps *SortedPeerset) Add(p peer.ID) bool {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.topKPeers[p]; ok {
		return false
	}
	if _, ok := ps.restOfPeers[p]; ok {
		return false
	}

	distance := ks.XORKeySpace.Key([]byte(p)).Distance(ps.from)
	pm := &peerMetricHeapItem{
		IPeerMetric: peerMetric{
			peer:   p,
			metric: distance,
		},
	}

	if ps.heapTopKPeers.Len() < ps.kvalue {
		heap.Push(&ps.heapTopKPeers, pm)
		ps.topKPeers[p] = pm
		return true
	}

	switch ps.heapTopKPeers.data[0].Metric().Cmp(distance) {
	case -1:
		heap.Push(&ps.heapRestOfPeers, pm)
		ps.restOfPeers[p] = pm
		return false
	case 1:
		bumpedPeer := heap.Pop(&ps.heapTopKPeers).(*peerMetricHeapItem)
		delete(ps.topKPeers, bumpedPeer.Peer())

		heap.Push(&ps.heapRestOfPeers, bumpedPeer)
		ps.restOfPeers[bumpedPeer.Peer()] = bumpedPeer

		heap.Push(&ps.heapTopKPeers, pm)
		ps.topKPeers[p] = pm
		return true
	default:
		return false
	}
}

func (ps *SortedPeerset) TopK() []peer.ID {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	topK := make([]peer.ID, 0, len(ps.heapTopKPeers.data))
	for _, pm := range ps.heapTopKPeers.data {
		topK = append(topK, pm.Peer())
	}

	return topK
}

func (ps *SortedPeerset) KUnqueried() []peer.ID {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	topK := make([]IPeerMetric, 0, len(ps.heapTopKPeers.data))
	for _, pm := range ps.heapTopKPeers.data {
		if _, ok := ps.queried[pm.Peer()]; !ok {
			topK = append(topK, pm.IPeerMetric)
		}
	}

	sortedPeers := ps.sortPeers(topK)
	peers := make([]peer.ID, 0, sortedPeers.Len())
	for i := range topK {
		p := sortedPeers.GetPeerID(i)
		peers = append(peers, p)
	}

	return peers
}

func (ps *SortedPeerset) MarkQueried(p peer.ID) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	ps.queried[p] = struct{}{}
}

func (ps *SortedPeerset) Remove(p peer.ID) {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	delete(ps.queried, p)

	if item, ok := ps.topKPeers[p]; ok {
		heap.Remove(&ps.heapTopKPeers, item.index)
		delete(ps.topKPeers, p)

		if len(ps.heapRestOfPeers.data) > 0 {
			upgrade := heap.Pop(&ps.heapRestOfPeers).(*peerMetricHeapItem)
			delete(ps.restOfPeers, upgrade.Peer())

			heap.Push(&ps.heapTopKPeers, upgrade)
			ps.topKPeers[upgrade.Peer()] = upgrade
		}
	} else if item, ok := ps.restOfPeers[p]; ok {
		heap.Remove(&ps.heapRestOfPeers, item.index)
		delete(ps.restOfPeers, p)
	}
}
