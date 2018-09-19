package borges

import (
	"errors"
	"io"
)

var (
	//ErrStop is used to stop a ForEach function in an Iter
	ErrStop = errors.New("stop iter")
)

type RepositoryIterator interface {
	Next() (*Repository, error)
	ForEach(func(*Repository) error) error
	Close()
}

// ForEachIterator is a helper function to build iterators without need to
// rewrite the same ForEach function each time.
func ForEachIterator(iter RepositoryIterator, cb func(*Repository) error) error {
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
			if err == ErrStop {
				return nil
			}

			return err
		}
	}
}
