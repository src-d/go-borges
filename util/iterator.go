package util

import (
	"io"

	borges "github.com/src-d/go-borges"
)

type LocationRepositoryIterator struct {
	mode borges.Mode
	locs []borges.Location
	iter borges.RepositoryIterator
}

func NewLocationRepositoryIterator(locs []borges.Location, mode borges.Mode) *LocationRepositoryIterator {
	return &LocationRepositoryIterator{locs: locs, mode: mode}
}

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

func (iter *LocationRepositoryIterator) ForEach(cb func(borges.Repository) error) error {
	return ForEachRepositoryIterator(iter, cb)
}

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
