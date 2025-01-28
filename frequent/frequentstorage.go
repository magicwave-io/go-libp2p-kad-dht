package frequent

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/routing"
	boom "github.com/libp2p/go-libp2p-kad-dht/frequent/boom"
	"github.com/multiformats/go-multihash"
	"paidpiper.com/payment-gateway/boom/data"
)

type Storage interface {
	Add(key []byte)
	AddMultihash(key []byte)
	GetMostFrequentContentAsync(ctx context.Context) <-chan routing.ContentListing
	Elements() []*data.FrequencyContentMetadata
}

func New() Storage {
	return &frequentStorage{
		frequentCIDs: boom.NewTopK(0.001, 0.99, 50*1000),
	}
}

type frequentStorage struct {
	frequentCIDs *boom.TopK
}

func (fs *frequentStorage) AddMultihash(key []byte) {
	_, m, err := multihash.MHFromBytes(key)
	if err != nil {
		fmt.Println("Debug_FREQ: MHFromBytes error")
		return
	}
	b58String := m.B58String()
	cid, err := cid.Parse(b58String)
	if err != nil {
		fmt.Println("Debug_FREQ: Parse CID error")
		return
	}
	fs.frequentCIDs.Add(cid.Bytes())
}
func (fs *frequentStorage) Add(key []byte) {
	c, err := cid.Parse(key)
	if err != nil {
		fmt.Println("Debug_FREQ: parse cid error")
		return
	}
	if c.Version() == 0 {
		fmt.Println("Debug_FREQ: version not valid")
		return
	}
	fs.frequentCIDs.Add(key)
}

func (fs *frequentStorage) GetMostFrequentContentAsync(ctx context.Context) <-chan routing.ContentListing {
	if fs.frequentCIDs == nil {
		topOut := make(chan routing.ContentListing)
		close(topOut)
		return topOut
	}

	ts := time.Now()

	defaultChannelBuffer := 1

	topOut := make(chan routing.ContentListing, defaultChannelBuffer)

	for _, e := range fs.frequentCIDs.Elements() {

		_, cid, err := cid.CidFromBytes(e.Data)

		if err != nil {
			listing := routing.ContentListing{
				Cid:         cid,
				Frequency:   e.Freq,
				LastUpdated: ts,
			}
			topOut <- listing
		}
	}

	return topOut
}

func (fs *frequentStorage) Elements() []*data.FrequencyContentMetadata {
	els := fs.frequentCIDs.Elements()
	listings := []*data.FrequencyContentMetadata{}
	ts := time.Now()
	for _, e := range els {
		l := &data.FrequencyContentMetadata{
			Cid:         e.Data,
			Frequency:   e.Freq,
			LastUpdated: ts,
		}
		listings = append(listings, l)
	}
	return listings
}
