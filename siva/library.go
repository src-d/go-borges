package siva

import (
	"fmt"
	"io"
	"strings"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
	billy "gopkg.in/src-d/go-billy.v4"
	butil "gopkg.in/src-d/go-billy.v4/util"
)

var _ borges.Library = new(Library)

// NewLibrary creates a new siva.Library.
func NewLibrary(id string, fs billy.Filesystem) *Library {
	return &Library{
		id: borges.LibraryID(id),
		fs: fs,
	}
}

// Library represents a borges.Library implementation based on siva files.
type Library struct {
	id borges.LibraryID
	fs billy.Filesystem
}

// ID implements borges.Library interface.
func (l *Library) ID() borges.LibraryID {
	panic("not implemented")
}

// Init implements borges.Library interface.
func (l *Library) Init(borges.RepositoryID) (borges.Repository, error) {
	panic("not implemented")
}

// Get implements borges.Library interface.
func (l *Library) Get(borges.RepositoryID, borges.Mode) (borges.Repository, error) {
	panic("not implemented")
}

// GetOrInit implements borges.Library interface.
func (l *Library) GetOrInit(borges.RepositoryID) (borges.Repository, error) {
	panic("not implemented")
}

// TODO: find if we have to use ".git" suffix for repository ids
func repoID(endpoint string) borges.RepositoryID {
	name, _ := borges.NewRepositoryID(endpoint)
	return borges.RepositoryID(strings.TrimSuffix(name.String(), ".git"))
}

func locationID(file string) borges.LocationID {
	id := strings.TrimSuffix(file, ".siva")
	return borges.LocationID(id)
}

// Has implements borges.Library interface.
func (l *Library) Has(name borges.RepositoryID) (bool, borges.LibraryID, borges.LocationID, error) {
	it, err := l.Locations()
	if err != nil {
		return false, "", "", err
	}
	defer it.Close()

	for {
		loc, err := it.Next()
		if err == io.EOF {
			return false, "", "", nil
		}
		if err != nil {
			return false, "", "", err
		}

		has, err := loc.Has(name)
		if err != nil {
			return false, "", "", err
		}

		if has {
			return true, l.id, loc.ID(), nil
		}
	}
}

// Repositories implements borges.Library interface.
func (l *Library) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	locs, err := l.locations()
	if err != nil {
		return nil, err
	}
	return util.NewLocationRepositoryIterator(locs, mode), nil
}

// Location implements borges.Library interface.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	path := fmt.Sprintf("%s.siva", id)
	return NewLocation(id, l.fs, path)
}

func (l *Library) locations() ([]borges.Location, error) {
	var locs []borges.Location

	sivas, err := butil.Glob(l.fs, "*.siva")
	if err != nil {
		return nil, err
	}

	for _, s := range sivas {
		loc, err := NewLocation(locationID(s), l.fs, s)
		if err != nil {
			continue
		}

		locs = append(locs, loc)
	}

	return locs, nil
}

// Locations implements borges.Library interface.
func (l *Library) Locations() (borges.LocationIterator, error) {
	locs, err := l.locations()
	if err != nil {
		return nil, err
	}
	return util.NewLocationIterator(locs), nil
}

// Library implements borges.Library interface.
func (l *Library) Library(borges.LibraryID) (borges.Library, error) {
	panic("not implemented")
}

// Libraries implements borges.Library interface.
func (l *Library) Libraries() (borges.LibraryIterator, error) {
	panic("not implemented")
}
