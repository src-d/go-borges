package plain

import (
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
)

// Library represents a borges.Library implementation based on billy.Filesystems.
type Library struct {
	l map[borges.LocationID]*Location
}

// NewLibrary returns a new empty Library instance.
func NewLibrary() *Library {
	return &Library{
		l: make(map[borges.LocationID]*Location, 0),
	}
}

// AddLocation adds a Location to the Library.
func (l *Library) AddLocation(location *Location) {
	l.l[location.ID()] = location
}

// GetOrInit is not implemented. It honors the borges.Library interface.
func (l *Library) GetOrInit(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Init is not implemented. It honors the borges.Library interface.
func (l *Library) Init(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has returns true and the LocationID if the given RepositoryID matches any
// repository at any location belonging to this Library.
func (l *Library) Has(id borges.RepositoryID) (bool, borges.LocationID, error) {
	for _, loc := range l.l {
		ok, err := loc.Has(id)
		if err != nil {
			return false, "", err
		}

		if ok {
			return true, loc.ID(), nil
		}
	}

	return false, ".", nil
}

// Get open a repository with the given RepositoryID, it itereates all the
// library locations until this repository is found. If a repository with the
// given RepositoryID can't be found the ErrRepositoryNotExists is returned.
func (l *Library) Get(id borges.RepositoryID, m borges.Mode) (borges.Repository, error) {
	for _, loc := range l.l {
		ok, err := loc.Has(id)
		if err != nil {
			return nil, err
		}

		if ok {
			return openRepository(loc, id, m)
		}
	}

	return nil, borges.ErrRepositoryNotExists.New(id)
}

// Location returns the a Location with the given ID, if exists, otherwise
// ErrLocationNotExists is returned.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	location, ok := l.l[id]
	if !ok {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return location, nil
}

// Repositories returns a RepositoryIterator that iterates through all the
// repositories contained in all Location contained in this Library.
func (l *Library) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	locs := make([]borges.Location, len(l.l))

	var i int
	for _, loc := range l.l {
		locs[i] = loc
		i++
	}

	return util.NewLocationRepositoryIterator(locs, mode), nil
}
