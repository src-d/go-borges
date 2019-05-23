package libraries

import (
	"io"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"github.com/src-d/go-borges/util"
)

// MergeRepositoryIterators builds a new iterator from the given ones.
func MergeRepositoryIterators(iters []borges.RepositoryIterator) borges.RepositoryIterator {
	return &repoIter{iters: iters}
}

type repoIter struct {
	iters []borges.RepositoryIterator
}

var _ borges.RepositoryIterator = (*repoIter)(nil)

// Next implements the borges.RepositoryIterator interface.
func (i *repoIter) Next() (borges.Repository, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	repo, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return repo, nil
}

// ForEach implements the borges.RepositoryIterator interface.
func (i *repoIter) ForEach(cb func(borges.Repository) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachRepositoryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.RepositoryIterator interface.
func (i *repoIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// MergeLocationIterators builds a new iterator from the given ones.
func MergeLocationIterators(iters []borges.LocationIterator) borges.LocationIterator {
	return &locationIter{iters: iters}
}

type locationIter struct {
	iters []borges.LocationIterator
}

var _ borges.LocationIterator = (*locationIter)(nil)

// Next implements the borges.LocationIterator interface.
func (i *locationIter) Next() (borges.Location, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	loc, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return loc, nil
}

// ForEach implements the borges.LocationIterator interface.
func (i *locationIter) ForEach(cb func(borges.Location) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLocatorIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.LocationIterator interface.
func (i *locationIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// MergeLibraryIterators builds a new iterator from the given ones.
func MergeLibraryIterators(iters []borges.LibraryIterator) borges.LibraryIterator {
	return &libIter{iters: iters}
}

type libIter struct {
	iters []borges.LibraryIterator
}

var _ borges.LibraryIterator = (*libIter)(nil)

// Next implements the borges.LibraryIterator interface.
func (i *libIter) Next() (borges.Library, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	lib, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return lib, nil
}

// ForEach implements the borges.LibraryIterator interface.
func (i *libIter) ForEach(cb func(borges.Library) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLibraryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.LibraryIterator interface.
func (i *libIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// RepositoryDefaultIter returns a borges.RepositoryIterator with no specific
// iteration order.
func RepositoryDefaultIter(
	l *Libraries,
	mode borges.Mode) (borges.RepositoryIterator, error) {

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

// RepoIterSivasJumpLocations returns a borges.RepositoryIterator which
// iters repositories only from siva.Library libraries. The repositories order
// will be all the repositories from a location from a different library, that
// is: repos from loc1/lib1, repos from loc1/lib2, repos from loc2/lib1, ...
func RepoIterSivasJumpLocations(
	libs *Libraries,
	mode borges.Mode) (borges.RepositoryIterator, error) {
	var filter FilterLibraryFunc = func(lib borges.Library) (bool, error) {
		_, ok := lib.(*siva.Library)
		return ok, nil
	}

	libIter, err := libs.FilteredLibraries(filter)
	if err != nil {
		return nil, err
	}

	var locsIter []*closedLocIter
	err = libIter.ForEach(func(lib borges.Library) error {
		locIter, err := lib.Locations()
		if err == nil {
			locsIter = append(locsIter, &closedLocIter{locIter, false})
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	var repos []borges.RepositoryIterator
	for !areClosed(locsIter) {
		for _, li := range locsIter {
			loc, err := li.Next()
			if err != nil {
				if err == io.EOF {
					li.Close()
					continue
				}

				return nil, err
			}

			ri, err := loc.Repositories(mode)
			if err != nil {
				return nil, err
			}

			repos = append(repos, ri)
		}
	}

	return MergeRepositoryIterators(repos), nil
}

func areClosed(locs []*closedLocIter) bool {
	if len(locs) == 0 {
		return true
	}

	for _, loc := range locs {
		if !loc.closed {
			return false
		}
	}

	return true
}

type closedLocIter struct {
	borges.LocationIterator

	closed bool
}

func (i *closedLocIter) Close() {
	if i.closed {
		return
	}

	i.closed = true
	i.LocationIterator.Close()
}
