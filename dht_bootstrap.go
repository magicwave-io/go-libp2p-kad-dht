package dht

import (
	"context"
	"fmt"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	process "github.com/jbenet/goprocess"
	processctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
	_ "github.com/multiformats/go-multiaddr-dns"
)

var DefaultBootstrapPeers []multiaddr.Multiaddr

// Minimum number of peers in the routing table. If we drop below this and we
// see a new peer, we trigger a bootstrap round.
var minRTRefreshThreshold = 4

func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		DefaultBootstrapPeers = append(DefaultBootstrapPeers, ma)
	}
}

// Start the refresh worker.
func (dht *IpfsDHT) startRefreshing() error {
	// scan the RT table periodically & do a random walk for cpl's that haven't been queried since the given period
	dht.proc.Go(func(proc process.Process) {
		ctx := processctx.OnClosingContext(proc)

		refreshTicker := time.NewTicker(dht.rtRefreshPeriod)
		defer refreshTicker.Stop()

		// refresh if option is set
		if dht.autoRefresh {
			dht.doRefresh(ctx)
		} else {
			// disable the "auto-refresh" ticker so that no more ticks are sent to this channel
			refreshTicker.Stop()
		}

		for {
			var waiting []chan<- error
			select {
			case <-refreshTicker.C:
			case res := <-dht.triggerRtRefresh:
				if res != nil {
					waiting = append(waiting, res)
				}
			case <-ctx.Done():
				return
			}

			// Batch multiple refresh requests if they're all waiting at the same time.
		collectWaiting:
			for {
				select {
				case res := <-dht.triggerRtRefresh:
					if res != nil {
						waiting = append(waiting, res)
					}
				default:
					break collectWaiting
				}
			}

			err := dht.doRefresh(ctx)
			for _, w := range waiting {
				w <- err
				close(w)
			}
			if err != nil {
				logger.Warning(err)
			}
		}
	})

	return nil
}

func (dht *IpfsDHT) doRefresh(ctx context.Context) error {
	var merr error
	if err := dht.selfWalk(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}
	if err := dht.refreshCpls(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}
	return merr
}

// refreshCpls scans the routing table, and does a random walk for cpl's that haven't been queried since the given period
func (dht *IpfsDHT) refreshCpls(ctx context.Context) error {
	doQuery := func(cpl uint, target string, f func(context.Context) error) error {
		logger.Infof("starting refreshing cpl %d to %s (routing table size was %d)",
			cpl, target, dht.routingTable.Size())
		defer func() {
			logger.Infof("finished refreshing cpl %d to %s (routing table size is now %d)",
				cpl, target, dht.routingTable.Size())
		}()
		queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
		defer cancel()
		err := f(queryCtx)
		if err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return nil
		}
		return err
	}

	trackedCpls := dht.routingTable.GetTrackedCplsForRefresh()

	var merr error
	for _, tcpl := range trackedCpls {
		if time.Since(tcpl.LastRefreshAt) <= dht.rtRefreshPeriod {
			continue
		}
		// gen rand peer with the cpl
		randPeer, err := dht.routingTable.GenRandPeerID(tcpl.Cpl)
		if err != nil {
			logger.Errorf("failed to generate peerID for cpl %d, err: %s", tcpl.Cpl, err)
			continue
		}

		// walk to the generated peer
		walkFnc := func(c context.Context) error {
			_, err := dht.FindPeer(c, randPeer)
			if err == routing.ErrNotFound {
				return nil
			}
			return err
		}

		if err := doQuery(tcpl.Cpl, randPeer.String(), walkFnc); err != nil {
			merr = multierror.Append(
				merr,
				fmt.Errorf("failed to do a random walk for cpl %d: %s", tcpl.Cpl, err),
			)
		}
	}
	return merr
}

// Traverse the DHT toward the self ID
func (dht *IpfsDHT) selfWalk(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, dht.rtRefreshQueryTimeout)
	defer cancel()
	_, err := dht.FindPeer(queryCtx, dht.self)
	if err == routing.ErrNotFound {
		return nil
	}
	return fmt.Errorf("failed to query self during routing table refresh: %s", err)
}

// Bootstrap tells the DHT to get into a bootstrapped state satisfying the
// IpfsRouter interface.
//
// This just calls `RefreshRoutingTable`.
func (dht *IpfsDHT) Bootstrap(_ context.Context) error {
	dht.RefreshRoutingTable()
	return nil
}

// RefreshRoutingTable tells the DHT to refresh it's routing tables.
//
// The returned channel will block until the refresh finishes, then yield the
// error and close. The channel is buffered and safe to ignore.
func (dht *IpfsDHT) RefreshRoutingTable() <-chan error {
	res := make(chan error, 1)
	dht.triggerRtRefresh <- res
	return res
}
