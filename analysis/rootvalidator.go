package analysis

import "github.com/ipfs/go-cid"

type RootValidator interface {
	CheckRoot(cid cid.Cid) (bool, error)
}
