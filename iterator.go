package borges

import (
	"errors"
)

var (
	//ErrStop is used to stop a ForEach function in an Iter
	ErrStop = errors.New("stop iter")
)

// RepositoryIterator represents a Repository iterator.
type RepositoryIterator interface {
	// Next returns the next repository from the iterator. If the iterator has
	// reached the end it will return io.EOF as an error.
	Next() (Repository, error)
	// ForEach call the function for each object contained on this iter until
	// an error happens or the end of the iter is reached. If ErrStop is sent
	// the iteration is stop but no error is returned. The iterator is closed.
	//
	// util.ForEachRepositoryIterator should be used to implement this function
	// unless that performance reason exists.
	ForEach(func(Repository) error) error
	// Close releases any resources used by the iterator.
	Close()
}

// LocationIterator represents a Location iterator.
type LocationIterator interface {
	// Next returns the next location from the iterator. If the iterator has
	// reached the end it will return io.EOF as an error.
	Next() (Location, error)
	// ForEach call the function for each object contained on this iter until
	// an error happens or the end of the iter is reached. If ErrStop is sent
	// the iteration is stop but no error is returned. The iterator is closed.
	//
	// util.ForEachLocatorIterator should be used to implement this function
	// unless that performance reason exists.
	ForEach(func(Location) error) error
	// Close releases any resources used by the iterator.
	Close()
}

// LibraryIterator represents a Location iterator.
type LibraryIterator interface {
	// Next returns the next library from the iterator. If the iterator has
	// reached the end it will return io.EOF as an error.
	Next() (Library, error)
	// ForEach call the function for each object contained on this iter until
	// an error happens or the end of the iter is reached. If ErrStop is sent
	// the iteration is stop but no error is returned. The iterator is closed.
	//
	// util.ForEachLibraryIterator should be used to implement this function
	// unless that performance reason exists.
	ForEach(func(Library) error) error
	// Close releases any resources used by the iterator.
	Close()
}
