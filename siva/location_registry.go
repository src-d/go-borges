package siva

import (
	"sync"

	borges "github.com/src-d/go-borges"

	lru "github.com/hashicorp/golang-lru"
)

// locationRegistry holds a list of locations that have a transaction under way
// and recently used.
type locationRegistry struct {
	used  map[borges.LocationID]*Location
	cache *lru.Cache

	m sync.RWMutex
}

func newLocationRegistry(cacheSize int) (*locationRegistry, error) {
	var c *lru.Cache
	var err error

	if cacheSize > 0 {
		c, err = lru.New(cacheSize)
		if err != nil {
			return nil, err
		}
	}

	return &locationRegistry{
		used:  make(map[borges.LocationID]*Location),
		cache: c,
	}, nil
}

// Get retrieves a location from the registry.
func (r *locationRegistry) Get(id borges.LocationID) (*Location, bool) {
	r.m.RLock()
	defer r.m.RUnlock()

	if l, ok := r.used[id]; ok {
		return l, true
	}

	if r.cache == nil {
		return nil, false
	}

	if l, ok := r.cache.Get(id); ok {
		return l.(*Location), true
	}

	return nil, false
}

// Add stores a location in the registry.
func (r *locationRegistry) Add(l *Location) {
	if r.cache == nil {
		return
	}

	r.m.RLock()
	defer r.m.RUnlock()

	r.cache.Add(l.ID(), l)
}

// StartTransaction marks a location as being used so it does not get evicted.
func (r *locationRegistry) StartTransaction(l *Location) {
	r.m.Lock()
	defer r.m.Unlock()

	r.used[l.ID()] = l
	if r.cache != nil {
		r.cache.Remove(l.ID())
	}
}

// EndTransaction moves a location back to normal cache.
func (r *locationRegistry) EndTransaction(l *Location) {
	r.m.Lock()
	defer r.m.Unlock()

	delete(r.used, l.ID())
	if r.cache != nil {
		r.cache.Add(l.ID(), l)
	}
}
