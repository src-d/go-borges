package util

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/index"
	"gopkg.in/src-d/go-git.v4/storage"
)

// ErrReadOnlyStorer error returns when a write method is used in a ReadOnlyStorer.
var ErrReadOnlyStorer = errors.NewKind("storer in read-only mode")

// ReadOnlyStorer it's a storer that simply fails when you try to do any kind
// of write operation over the storer.
type ReadOnlyStorer struct {
	storage.Storer
}

// SetEncodedObject honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) SetEncodedObject(plumbing.EncodedObject) (plumbing.Hash, error) {
	return plumbing.ZeroHash, ErrReadOnlyStorer.New()
}

// CheckAndSetReference honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) CheckAndSetReference(new *plumbing.Reference, old *plumbing.Reference) error {
	return ErrReadOnlyStorer.New()
}

// SetReference honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) SetReference(*plumbing.Reference) error {
	return ErrReadOnlyStorer.New()
}

// SetShallow honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) SetShallow([]plumbing.Hash) error {
	return ErrReadOnlyStorer.New()

}

// SetIndex honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) SetIndex(*index.Index) error {
	return ErrReadOnlyStorer.New()

}

// SetConfig honors the storage.Storer interface. It fails with a
// ErrReadOnlyStorer when is called.
func (s *ReadOnlyStorer) SetConfig(*config.Config) error {
	return ErrReadOnlyStorer.New()

}
