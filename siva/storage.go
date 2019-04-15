package siva

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/src-d/go-borges/util"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	butil "gopkg.in/src-d/go-billy.v4/util"
	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"gopkg.in/src-d/go-git.v4/storage/transactional"
)

const (
	transactionName = "transaction"
	baseSivaName    = "base"
	packedRefsPath  = "packed-refs"
	refsPath        = "refs"
)

// ErrEmptyCommit is returned when a repository opened in RW mode tries to commit no changes.
var ErrEmptyCommit = errors.NewKind("there weren't changes to commit")

// ReadOnlyStorer is a wrapper for util.ReadOnlyStorer
type ReadOnlyStorer struct {
	// This wrapper is necessary becuse when a git.Open is performed there must be a
	// HEAD reference. Since we have rooted repositories packed in siva files without
	// a HEAD file we need to overwrite the Reference method to be able to return a
	// reference even if there's no any HEAD.
	util.ReadOnlyStorer
}

// NewReadOnlyStorer returns a new *ReadOnlyStorer initialized with the given storage.Storer.
func NewReadOnlyStorer(sto storage.Storer) *ReadOnlyStorer {
	return &ReadOnlyStorer{util.ReadOnlyStorer{Storer: sto}}
}

// Reference implements the storer.ReferenceStorer interface.
func (s *ReadOnlyStorer) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	if ref, err := s.ReadOnlyStorer.Reference(name); err == nil || name != plumbing.HEAD {
		return ref, err
	}

	if master, err := s.ReadOnlyStorer.Reference(plumbing.Master); err == nil {
		return master, err
	}

	return plumbing.NewHashReference(
		plumbing.ReferenceName("refs/heads/zero"), plumbing.ZeroHash,
	), nil
}

// Storage holds a ReadWrite siva storage. It can be transactional in which
// case it will write to a temporary siva file and will append it to the
// original siva on Commit.
type Storage struct {
	storage.Storer

	memory.ReferenceStorage
	dirtyRefs bool

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
	refIter, err := baseStorage.IterReferences()
	if err != nil {
		return nil, err
	}

	refSto, err := newRefStorage(refIter)
	if err != nil {
		return nil, err
	}

	if !transaction {
		return &Storage{
			Storer:           baseStorage,
			ReferenceStorage: refSto,
			base:             base,
			path:             path,
			fs:               baseFS,
			tmp:              tmp,
			tmpDir:           rootDir,
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
		Storer:           sto,
		ReferenceStorage: refSto,
		base:             base,
		path:             path,
		fs:               transactionFS,
		tmp:              tmp,
		tmpDir:           rootDir,
	}, nil
}

// Commit finishes the writes on a Storage. If transactional mode is enabled
// the backing transaction siva finishes writing and it is appended to the
// original siva file. If it's not transactional the original siva file is
// closed.
func (s *Storage) Commit() error {
	defer s.Cleanup()

	if err := s.PackRefs(); err != nil {
		return err
	}

	err := s.fs.Sync()
	if err != nil {
		return err
	}

	_, ok := s.Storer.(transactional.Storage)
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
		if os.IsNotExist(err) {
			// if there's no a transaction file is because
			// none write operations were performed
			err = ErrEmptyCommit.Wrap(err)
		}

		return err
	}

	_, err = io.Copy(dest, source)
	if err != nil {
		return err
	}

	return nil
}

// PackfileWriter implements storer.PackfileWriter interface.
func (s *Storage) PackfileWriter() (io.WriteCloser, error) {
	p, ok := s.Storer.(storer.PackfileWriter)
	if !ok {
		return nil, git.ErrPackedObjectsNotSupported
	}

	return p.PackfileWriter()
}

// Close finishes writes to siva file and cleans up temporary storage.
func (s *Storage) Close() (err error) {
	if pErr := s.PackRefs(); pErr != nil {
		err = pErr
	}

	if sErr := s.Sync(); sErr != nil {
		err = sErr
	}

	s.Cleanup()
	return err
}

// Cleanup deletes temporary files created for this Storage.
func (s *Storage) Cleanup() error {
	return butil.RemoveAll(s.tmp, s.tmpDir)
}

// Sync closes the siva file where the storer is writing.
func (s *Storage) Sync() error {
	return s.fs.Sync()
}

// Filesystem returns the filesystem that can be used for writing.
func (s *Storage) Filesystem() billy.Filesystem {
	return s.fs
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

func newRefStorage(iter storer.ReferenceIter) (memory.ReferenceStorage, error) {
	rs := map[plumbing.ReferenceName]*plumbing.Reference{}
	err := iter.ForEach(func(r *plumbing.Reference) error {
		rs[r.Name()] = r
		return nil
	})

	if err != nil {
		return nil, err
	}

	if _, ok := rs[plumbing.HEAD]; !ok {
		r, ok := rs[plumbing.Master]
		if !ok {
			r = plumbing.NewHashReference(
				plumbing.ReferenceName("refs/heads/zero"),
				plumbing.ZeroHash,
			)
		}

		rs[plumbing.HEAD] = r
	}

	return rs, nil
}

// SetReference implements storer.ReferenceStorer.
func (s *Storage) SetReference(ref *plumbing.Reference) error {
	s.dirtyRefs = true
	return s.ReferenceStorage.SetReference(ref)
}

// CheckAndSetReference implements storer.ReferenceStorer.
func (s *Storage) CheckAndSetReference(new, old *plumbing.Reference) error {
	s.dirtyRefs = true
	return s.ReferenceStorage.CheckAndSetReference(new, old)
}

// Reference implements storer.ReferenceStorer.
func (s *Storage) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	return s.ReferenceStorage.Reference(name)
}

// IterReferences implements storer.ReferenceStorer.
func (s *Storage) IterReferences() (storer.ReferenceIter, error) {
	return s.ReferenceStorage.IterReferences()
}

// RemoveReference implements storer.ReferenceStorer.
func (s *Storage) RemoveReference(name plumbing.ReferenceName) error {
	s.dirtyRefs = true
	return s.ReferenceStorage.RemoveReference(name)
}

// CountLooseRefs implements storer.ReferenceStorer.
func (s *Storage) CountLooseRefs() (int, error) {
	return s.ReferenceStorage.CountLooseRefs()
}

// PackRefs packs the references kept in memory and write them to the siva storage.
func (s *Storage) PackRefs() (err error) {
	if !s.dirtyRefs {
		return nil
	}

	if len(s.ReferenceStorage) == 0 {
		err := s.fs.Remove(packedRefsPath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		return nil
	}

	f, err := s.fs.OpenFile(packedRefsPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	defer func() {
		err = f.Close()
	}()

	refs := make([]*plumbing.Reference, 0, len(s.ReferenceStorage))
	for _, r := range s.ReferenceStorage {
		if r.Name() != plumbing.HEAD {
			refs = append(refs, r)
		}
	}

	if len(refs) == 0 {
		return nil
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].Name() > refs[j].Name()
	})

	for _, r := range refs {
		entry := []byte(fmt.Sprintln(r.String()))
		if _, err := f.Write(entry); err != nil {
			return err
		}
	}

	return butil.RemoveAll(s.fs, refsPath)
}
