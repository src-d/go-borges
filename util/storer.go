package util

import (
	borges "github.com/src-d/go-borges"
	billy "gopkg.in/src-d/go-billy.v4"
	butil "gopkg.in/src-d/go-billy.v4/util"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/format/index"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/transactional"
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

// RepositoryStorer wraps the storer to make it read only or transactional.
func RepositoryStorer(
	fs billy.Filesystem,
	tmpFS billy.Filesystem,
	mode borges.Mode,
	transactional bool,
) (storage.Storer, string, error) {
	s := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())

	switch mode {
	case borges.ReadOnlyMode:
		return &ReadOnlyStorer{s}, "", nil
	case borges.RWMode:
		if transactional {
			return repositoryTemporalStorer(tmpFS, s)
		}

		return s, "", nil
	default:
		return nil, "", borges.ErrModeNotSupported.New(mode)
	}
}

func repositoryTemporalStorer(
	tmpFS billy.Filesystem,
	parent storage.Storer,
) (storage.Storer, string, error) {
	tempPath, err := butil.TempDir(tmpFS, "transactions", "")
	if err != nil {
		return nil, "", err
	}

	tfs, err := tmpFS.Chroot(tempPath)
	if err != nil {
		return nil, "", err
	}

	ts := filesystem.NewStorage(tfs, cache.NewObjectLRUDefault())
	s := transactional.NewStorage(parent, ts)

	return s, tempPath, nil
}
