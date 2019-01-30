package util

import (
	"io"

	"github.com/src-d/go-borges"
)

// LocationRepositoryIterator iterates the repositories from a list of
// borges.Location.
type LocationRepositoryIterator struct {
	mode borges.Mode
	locs []borges.Location
	iter borges.RepositoryIterator
}

// NewLocationRepositoryIterator returns a new borges.RepositoryIterator from
// a list of borges.Location.
func NewLocationRepositoryIterator(locs []borges.Location, mode borges.Mode) *LocationRepositoryIterator {
	return &LocationRepositoryIterator{locs: locs, mode: mode}
}

// Next returns the next repository from the iterator. If the iterator has
// reached the end it will return io.EOF as an error.
func (iter *LocationRepositoryIterator) Next() (borges.Repository, error) {
	if len(iter.locs) == 0 {
		return nil, io.EOF
	}

	if iter.iter == nil {
		var err error
		iter.iter, err = iter.locs[0].Repositories(iter.mode)
		if err != nil {
			return nil, err
		}
	}

	r, err := iter.iter.Next()
	if err == io.EOF {
		iter.locs = iter.locs[1:]
		iter.iter = nil
		return iter.Next()
	}

	return r, err
}

// ForEach call the function for each object contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stop but no error is returned. The iterator is closed.
func (iter *LocationRepositoryIterator) ForEach(cb func(borges.Repository) error) error {
	return ForEachRepositoryIterator(iter, cb)
}

// Close releases any resources used by the iterator.
func (iter *LocationRepositoryIterator) Close() {}

// ForEachRepositoryIterator is a helper function to build iterators without
// need to rewrite the same ForEach function each time.
func ForEachRepositoryIterator(iter borges.RepositoryIterator, cb func(borges.Repository) error) error {
	defer iter.Close()
	for {
		r, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := cb(r); err != nil {
			if err == borges.ErrStop {
				return nil
			}

			return err
		}
	}
}

// LocationIterator iterates a list of borges.Location.
type LocationIterator struct {
	locs []borges.Location
}

// NewLocationIterator returns a LocationIterator based on a list of locations.
func NewLocationIterator(locs []borges.Location) *LocationIterator {
	return &LocationIterator{locs: locs}
}

// Next returns the next location from the iterator. If the iterator has
// reached the end it will return io.EOF as an error.
func (iter *LocationIterator) Next() (borges.Location, error) {
	if len(iter.locs) == 0 {
		return nil, io.EOF
	}

	var next borges.Location
	next, iter.locs = iter.locs[0], iter.locs[1:]
	return next, nil
}

// ForEach call the function for each object contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stop but no error is returned. The iterator is closed.
func (iter *LocationIterator) ForEach(cb func(borges.Location) error) error {
	return ForEachLocatorIterator(iter, cb)
}

// Close releases any resources used by the iterator.
func (iter *LocationIterator) Close() {}

// ForEachLocatorIterator is a helper function to build iterators without
// need to rewrite the same ForEach function each time.
func ForEachLocatorIterator(iter borges.LocationIterator, cb func(borges.Location) error) error {
	defer iter.Close()
	for {
		r, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := cb(r); err != nil {
			if err == borges.ErrStop {
				return nil
			}

			return err
		}
	}
}

// LibraryIterator iterates a list of borges.Library.
type LibraryIterator struct {
	libs []borges.Library
}

// NewLibraryIterator returns a LibraryIterator based on a list of libraries.
func NewLibraryIterator(libs []borges.Library) *LibraryIterator {
	return &LibraryIterator{libs: libs}
}

// Next returns the next location from the iterator. If the iterator has
// reached the end it will return io.EOF as an error.
func (iter *LibraryIterator) Next() (borges.Library, error) {
	if len(iter.libs) == 0 {
		return nil, io.EOF
	}

	var next borges.Library
	next, iter.libs = iter.libs[0], iter.libs[1:]
	return next, nil
}

// ForEach call the function for each object contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stop but no error is returned. The iterator is closed.
func (iter *LibraryIterator) ForEach(cb func(borges.Library) error) error {
	return ForEachLibraryIterator(iter, cb)
}

// Close releases any resources used by the iterator.
func (iter *LibraryIterator) Close() {}

// ForEachLibraryIterator is a helper function to build iterators without
// need to rewrite the same ForEach function each time.
func ForEachLibraryIterator(iter borges.LibraryIterator, cb func(borges.Library) error) error {
	defer iter.Close()
	for {
		r, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			return err
		}

		if err := cb(r); err != nil {
			if err == borges.ErrStop {
				return nil
			}

			return err
		}
	}
}
