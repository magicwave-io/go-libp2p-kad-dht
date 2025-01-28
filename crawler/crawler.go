package crawler

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"

	logging "github.com/ipfs/go-log"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
)

var logger = logging.Logger("dht-crawler")

type Crawler struct {
	parallelism int
	host        host.Host
	dhtRPC      *dht.ProtocolMessenger
}

func New(host host.Host, opts ...Option) (*Crawler, error) {
	o := new(options)
	if err := defaults(o); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}

	return &Crawler{
		parallelism: o.parallelism,
		host:        host,
		dhtRPC:      dht.NewProtocolMessenger(host, o.protocols, nil),
	}, nil
}

type HandleQueryResult func(p peer.ID, rtPeers []*peer.AddrInfo)
type HandleQueryFail func(p peer.ID, err error)

func (c *Crawler) Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess HandleQueryResult, handleFail HandleQueryFail) {
	jobs := make(chan peer.ID, 1)
	results := make(chan *queryResult, 1)

	// Start worker goroutines
	var wg sync.WaitGroup
	wg.Add(c.parallelism)
	for i := 0; i < c.parallelism; i++ {
		go func() {
			for p := range jobs {
				res := queryPeer(ctx, c.host, c.dhtRPC, p)
				results <- res
			}
		}()
	}

	defer wg.Done()
	defer close(jobs)

	toDial := make([]*peer.AddrInfo, 0, 1000)
	peersSeen := make(map[peer.ID]struct{})

	for _, ai := range startingPeers {
		toDial = append(toDial, ai)
		peersSeen[ai.ID] = struct{}{}
	}

	numQueried := 0
	outstanding := 0

	for len(toDial) > 0 || outstanding > 0 {
		var jobCh chan peer.ID
		var nextPeerID peer.ID
		if len(toDial) > 0 {
			jobCh = jobs
			nextPeerID = toDial[0].ID
		}

		select {
		case res := <-results:
			if len(res.data) > 0 {
				logger.Debugf("peer %v had %d peers", res.peer, len(res.data))
				rtPeers := make([]*peer.AddrInfo, 0, len(res.data))
				for p, ai := range res.data {
					c.host.Peerstore().AddAddrs(p, ai.Addrs, time.Hour)
					if _, ok := peersSeen[p]; !ok {
						peersSeen[p] = struct{}{}
						toDial = append(toDial, ai)
					}
					rtPeers = append(rtPeers, ai)
				}
				if handleSuccess != nil {
					handleSuccess(res.peer, rtPeers)
				}
			} else if handleFail != nil {
				handleFail(res.peer, res.err)
			}
			outstanding--
		case jobCh <- nextPeerID:
			outstanding++
			numQueried++
			toDial = toDial[1:]
			logger.Debugf("starting %d out of %d", numQueried, len(peersSeen))
		}
	}
}

type queryResult struct {
	peer peer.ID
	data map[peer.ID]*peer.AddrInfo
	err  error
}

func queryPeer(ctx context.Context, host host.Host, dhtRPC *dht.ProtocolMessenger, nextPeer peer.ID) *queryResult {
	tmpRT, err := kbucket.NewRoutingTable(20, kbucket.ConvertPeerID(nextPeer), time.Hour, host.Peerstore(), time.Hour)
	if err != nil {
		logger.Errorf("error creating rt for peer %v : %v", nextPeer, err)
		return &queryResult{nextPeer, nil, err}
	}

	connCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	err = host.Connect(connCtx, peer.AddrInfo{ID: nextPeer})
	if err != nil {
		logger.Errorf("could not connect to peer %v: %v", nextPeer, err)
		return &queryResult{nextPeer, nil, err}
	}

	localPeers := make(map[peer.ID]*peer.AddrInfo)
	var retErr error
	for cpl := 0; cpl <= 15; cpl++ {
		generatePeer, err := tmpRT.GenRandPeerID(uint(cpl))
		if err != nil {
			panic(err)
		}
		peers, err := dhtRPC.GetClosestPeers(ctx, nextPeer, generatePeer)
		if err != nil {
			logger.Debugf("error finding data on peer %v with cpl %d : %v", nextPeer, cpl, err)
			retErr = err
			break
		}
		for _, ai := range peers {
			if _, ok := localPeers[ai.ID]; !ok {
				localPeers[ai.ID] = ai
			}
		}
	}
	return &queryResult{nextPeer, localPeers, retErr}
}
