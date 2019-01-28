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
	ForEach(func(Repository) error) error
	// Close releases any resources used by the iterator.
	Close()
}
