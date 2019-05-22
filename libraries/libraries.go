package libraries

import (
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrLibraryExists an error returned when a borges.Library
	// added before is attempted to be added again.
	ErrLibraryExists = errors.NewKind("library %s already exists")
)

// FilterLibraryFunc stands for a borges.Library filter function.
type FilterLibraryFunc func(borges.Library) (bool, error)

// Libraries is an implementation to aggregate borges.Library in just one instance.
// The borges.Library that will be added shouldn't contain other libraries inside.
type Libraries struct {
	libs map[borges.LibraryID]borges.Library
}

var _ borges.Library = (*Libraries)(nil)

// New create a new Libraries instance.
func New() *Libraries {
	return &Libraries{
		libs: map[borges.LibraryID]borges.Library{},
	}
}

// Add adds a new borges.Library. It will fail with ErrLibraryExists
// if the library was already added.
func (l *Libraries) Add(lib borges.Library) error {
	_, ok := l.libs[lib.ID()]
	if ok {
		return ErrLibraryExists.New(lib.ID())
	}

	l.libs[lib.ID()] = lib
	return nil
}

// ID implements the Library interface.
func (l *Libraries) ID() borges.LibraryID {
	return ""
}

// Init implements the Library interface.
func (l *Libraries) Init(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Get implements the Library interface.
func (l *Libraries) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	var repos []borges.Repository
	for _, lib := range l.libs {
		r, err := lib.Get(id, mode)
		if err != nil {
			if borges.ErrRepositoryNotExists.Is(err) {
				continue
			}

			return nil, err
		}

		repos = append(repos, r)
	}

	if len(repos) == 0 {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return repos[0], nil
}

// GetOrInit implements the Library interface.
func (l *Libraries) GetOrInit(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has implements the Library interface.
func (l *Libraries) Has(id borges.RepositoryID) (bool, borges.LibraryID, borges.LocationID, error) {
	var matches []*hasReturnValues
	for _, lib := range l.libs {
		has, libID, locID, err := lib.Has(id)
		if err != nil {
			return false, "", "", err
		}

		if has {
			matches = append(matches, &hasReturnValues{
				libID: libID,
				locID: locID,
			})
		}
	}

	if len(matches) == 0 {
		return false, "", "", nil
	}

	return true, matches[0].libID, matches[0].locID, nil
}

type hasReturnValues struct {
	libID borges.LibraryID
	locID borges.LocationID
}

// Repositories implements the Library interface.
func (l *Libraries) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	var repositories []borges.RepositoryIterator
	for _, lib := range l.libs {
		repos, err := lib.Repositories(mode)
		if err != nil {
			return nil, err
		}

		repositories = append(repositories, repos)
	}

	return MergeRepositoryIterators(repositories), nil
}

// Location implements the Library interface.
func (l *Libraries) Location(id borges.LocationID) (borges.Location, error) {
	var locations []borges.Location
	for _, lib := range l.libs {
		loc, err := lib.Location(id)
		if err != nil {
			if borges.ErrLocationNotExists.Is(err) {
				continue
			}

			return nil, err
		}

		locations = append(locations, loc)
	}

	if len(locations) == 0 {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return locations[0], nil
}

// Locations implements the Library interface.
func (l *Libraries) Locations() (borges.LocationIterator, error) {
	var locations []borges.LocationIterator
	for _, lib := range l.libs {
		locs, err := lib.Locations()
		if err != nil {
			return nil, err
		}

		locations = append(locations, locs)
	}

	return MergeLocationIterators(locations), nil
}

// Library implements the Library interface.
func (l *Libraries) Library(id borges.LibraryID) (borges.Library, error) {
	lib, ok := l.libs[id]
	if !ok {
		return nil, borges.ErrLibraryNotExists.New(id)
	}

	return lib, nil
}

// Libraries implements the Library interface.
func (l *Libraries) Libraries() (borges.LibraryIterator, error) {
	return l.FilteredLibraries(func(borges.Library) (bool, error) {
		return true, nil
	})
}

// FilteredLibraries returns an iterator containing only those libraries
// accomplishing with the FilteredLibraryFunc function.
func (l *Libraries) FilteredLibraries(filter FilterLibraryFunc) (borges.LibraryIterator, error) {
	libs, err := l.libraries(filter)
	if err != nil {
		return nil, err
	}

	return util.NewLibraryIterator(libs), nil
}

func (l *Libraries) libraries(filter FilterLibraryFunc) ([]borges.Library, error) {
	libs := make([]borges.Library, 0, len(l.libs))
	for _, lib := range l.libs {
		ok, err := filter(lib)
		if err != nil {
			return nil, err
		}

		if ok {
			libs = append(libs, lib)
		}
	}

	return libs, nil
}
