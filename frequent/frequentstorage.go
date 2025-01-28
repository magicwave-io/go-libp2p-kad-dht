package frequent

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/routing"
	boom "github.com/libp2p/go-libp2p-kad-dht/frequent/boom"
	"paidpiper.com/payment-gateway/boom/data"
)

type Storage interface {
	Add(key []byte)
	GetMostFrequentContentAsync(ctx context.Context) <-chan routing.ContentListing
	Elements() []*data.FrequencyContentMetadata
}

func New() Storage {
	return &frequentStorage{
		frequentCIDs: boom.NewTopK(0.001, 0.99, 50),
	}
}

type frequentStorage struct {
	frequentCIDs *boom.TopK
}

func (fs *frequentStorage) Add(key []byte) {
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
