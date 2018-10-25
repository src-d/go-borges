package plain

import "github.com/src-d/go-borges"

type Library struct {
	l map[borges.LocationID]*Location
}

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
func (l *Library) GetOrInit(borges.RepositoryID) (*borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Init is not implemented. It honors the borges.Library interface.
func (l *Library) Init(borges.RepositoryID) (*borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has return true and the LocationID if a repository with the given ID exists
// on the Libary.
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

// Get opens a repository with the given id in the given mode.
func (l *Library) Get(id borges.RepositoryID, m borges.Mode) (*borges.Repository, error) {
	for _, loc := range l.l {
		ok, err := loc.Has(id)
		if err != nil {
			return nil, err
		}

		if ok {
			return loc.doGet(id, m)
		}
	}

	return nil, borges.ErrRepositoryNotExists.New(id)
}

// Location returns the a Location with the given ID, if exists.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	location, ok := l.l[id]
	if !ok {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return location, nil
}

func (l *Library) Repositories(borges.Mode) (borges.RepositoryIterator, error) {
	return nil, nil
}
