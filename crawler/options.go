package crawler

import (
	"github.com/libp2p/go-libp2p-core/protocol"
	"time"
)

// Option DHT Crawler option type.
type Option func(*options) error

type options struct {
	protocols     []protocol.ID
	parallelism   int
	crawlInterval time.Duration
}

// defaults are the default crawler options. This option will be automatically
// prepended to any options you pass to the crawler constructor.
var defaults = func(o *options) error {
	o.protocols = []protocol.ID{"/ipfs/kad/1.0.0"}
	o.parallelism = 1000
	o.crawlInterval = time.Hour

	return nil
}

// WithProtocols defines the ordered set of protocols the crawler will use to talk to other nodes
func WithProtocols(protocols []protocol.ID) Option {
	return func(o *options) error {
		o.protocols = protocols
		return nil
	}
}

// WithParallelism defines the number of queries that can be issued in parallel
func WithParallelism(parallelism int) Option {
	return func(o *options) error {
		o.parallelism = parallelism
		return nil
	}
}
