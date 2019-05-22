package libraries

import (
	"io"

	"github.com/src-d/go-borges"
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

// Next implementes the borges.RepositoryIterator interface.
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

// ForEach implementes the borges.RepositoryIterator interface.
func (i *repoIter) ForEach(cb func(borges.Repository) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachRepositoryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implementes the borges.RepositoryIterator interface.
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

// Next implementes the borges.LocationIterator interface.
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

// ForEach implementes the borges.LocationIterator interface.
func (i *locationIter) ForEach(cb func(borges.Location) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLocatorIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implementes the borges.LocationIterator interface.
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

// Next implementes the borges.LibraryIterator interface.
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

// ForEach implementes the borges.LibraryIterator interface.
func (i *libIter) ForEach(cb func(borges.Library) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLibraryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implementes the borges.LibraryIterator interface.
func (i *libIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}
