package siva

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	butil "gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/transactional"
)

const (
	transactionName = "transaction"
	baseSivaName    = "base"
)

// Storage holds a ReadWrite siva storage. It can be transactional in which
// case it will write to a temporary siva file and will append it to the
// original siva on Commit.
type Storage struct {
	storage.Storer

	base   billy.Filesystem
	path   string
	fs     sivafs.SivaFS
	tmp    billy.Filesystem
	tmpDir string
}

// NewStorage creates a new Storage struct. A new temporary directory is created
// for the siva filesystem that can be later deleted with Cleanup.
func NewStorage(
	base billy.Filesystem,
	path string,
	tmp billy.Filesystem,
	transaction bool,
) (*Storage, error) {
	rootDir, err := butil.TempDir(tmp, "/", "go-borges")
	if err != nil {
		return nil, err
	}

	cleanup := func() {
		butil.RemoveAll(tmp, rootDir)
	}

	rootFS, err := tmp.Chroot(rootDir)
	if err != nil {
		cleanup()
		return nil, err
	}

	c := cache.NewObjectLRUDefault()

	baseFS, err := getSivaFS(base, path, rootFS, baseSivaName)
	if err != nil {
		cleanup()
		return nil, err
	}
	baseStorage := filesystem.NewStorage(baseFS, c)

	if !transaction {
		return &Storage{
			Storer: baseStorage,
			base:   base,
			path:   path,
			fs:     baseFS,
			tmp:    tmp,
			tmpDir: rootDir,
		}, nil
	}

	transactionFS, err := getSivaFS(
		rootFS, transactionName,
		rootFS, transactionName,
	)
	if err != nil {
		cleanup()
		return nil, err
	}
	transactionStorage := filesystem.NewStorage(transactionFS, c)

	sto := transactional.NewStorage(baseStorage, transactionStorage)

	return &Storage{
		Storer: sto,
		base:   base,
		path:   path,
		fs:     transactionFS,
		tmp:    tmp,
		tmpDir: rootDir,
	}, nil
}

// Commit finishes the writes on a Storage. If transactional mode is enabled
// the backing transaction siva finishes writing and it is appended to the
// original siva file. If it's not transactional the original siva file is
// closed.
func (s *Storage) Commit() error {
	defer s.Cleanup()

	err := s.fs.Sync()
	if err != nil {
		return err
	}

	_, ok := s.Storer.(*transactional.Storage)
	if !ok {
		return nil
	}

	dest, err := s.base.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0)
	if err != nil {
		return err
	}

	transaction := filepath.Join(s.tmpDir, transactionName)
	source, err := s.tmp.Open(transaction)
	if err != nil {
		return err
	}

	_, err = io.Copy(dest, source)
	if err != nil {
		return err
	}

	return nil
}

// Close finishes writes to siva file and cleans up temporary storage.
func (s *Storage) Close() error {
	defer s.Cleanup()
	return s.Sync()
}

// Cleanup deletes temporary files created for this Storage.
func (s *Storage) Cleanup() error {
	return butil.RemoveAll(s.tmp, s.tmpDir)
}

// Sync closes the siva file where the storer is writing.
func (s *Storage) Sync() error {
	return s.fs.Sync()
}

func getSivaFS(
	base billy.Filesystem,
	path string,
	tmp billy.Filesystem,
	name string,
) (sivafs.SivaFS, error) {
	sivaTmp := fmt.Sprintf("%s-tmp", name)
	err := tmp.MkdirAll(sivaTmp, 0740)
	if err != nil {
		return nil, err
	}
	sivaTmpFS, err := tmp.Chroot(sivaTmp)
	if err != nil {
		return nil, err
	}

	return sivafs.NewFilesystem(base, path, sivaTmpFS)
}
