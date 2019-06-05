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
	"gopkg.in/src-d/go-git.v4/config"
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
	keepFile        = ".keep"
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

	refs   memory.ReferenceStorage
	config *config.Config
}

// NewReadOnlyStorer returns a new *ReadOnlyStorer initialized with the
// given storage.Storer.
func NewReadOnlyStorer(sto storage.Storer) (*ReadOnlyStorer, error) {
	refIter, err := sto.IterReferences()
	if err != nil {
		return nil, err
	}

	refSto, err := newRefStorage(refIter)
	if err != nil {
		return nil, err
	}

	return &ReadOnlyStorer{
		ReadOnlyStorer: util.ReadOnlyStorer{Storer: sto},
		refs:           refSto,
	}, nil
}

// NewReadOnlyStorerInitialized creates a new *ReadOnlyStorer with preloaded
// references and git config.
func NewReadOnlyStorerInitialized(
	sto storage.Storer,
	refs memory.ReferenceStorage,
	config *config.Config,
) (*ReadOnlyStorer, error) {
	return &ReadOnlyStorer{
		ReadOnlyStorer: util.ReadOnlyStorer{Storer: sto},
		refs:           refs,
		config:         config,
	}, nil
}

// Reference implements the storer.ReferenceStorer interface.
func (s *ReadOnlyStorer) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	if ref, err := s.refs.Reference(name); err == nil || name != plumbing.HEAD {
		return ref, err
	}

	if master, err := s.refs.Reference(plumbing.Master); err == nil {
		return master, err
	}

	return plumbing.NewHashReference(
		plumbing.ReferenceName("refs/heads/zero"), plumbing.ZeroHash,
	), nil
}

// IterReferences implements storer.ReferenceStorer.
func (s *ReadOnlyStorer) IterReferences() (storer.ReferenceIter, error) {
	return s.refs.IterReferences()
}

// CountLooseRefs implements storer.ReferenceStorer.
func (s *ReadOnlyStorer) CountLooseRefs() (int, error) {
	return s.refs.CountLooseRefs()
}

// Close implements io.Closer interface.
func (s *ReadOnlyStorer) Close() error {
	if c, ok := s.ReadOnlyStorer.Storer.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

// Config implements config.ConfigStorer interface.
func (s *ReadOnlyStorer) Config() (*config.Config, error) {
	if s.config == nil {
		c, err := s.ReadOnlyStorer.Config()
		if err != nil {
			return nil, err
		}
		s.config = c
	}

	return s.config, nil
}

// Committer interface has transactional Commit and Close methods for a storer.
type Committer interface {
	// Commit applies the changes to a storer if it's in transactional mode.
	Commit() error
	// Close signals the end of usage of a storer. If it's in transactional
	// mode this means rollback.
	Close() error
}

// Storage holds a ReadWrite siva storage. It can be transactional in which
// case it will write to a temporary siva file and will append it to the
// original siva on Commit.
type Storage struct {
	storage.Storer

	memory.ReferenceStorage
	dirtyRefs bool

	base          billy.Filesystem
	path          string
	baseFS        sivafs.SivaFS
	transFS       sivafs.SivaFS
	tmp           billy.Filesystem
	tmpDir        string
	transactional bool
	syncBase      bool
}

// NewStorage creates a new Storage struct. A new temporary directory is created
// for the siva filesystem that can be later deleted with Cleanup.
func NewStorage(
	base billy.Filesystem,
	path string,
	tmp billy.Filesystem,
	transaction bool,
	cache cache.Object,
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

	baseFS, err := getSivaFS(base, path, rootFS, baseSivaName)
	if err != nil {
		cleanup()
		return nil, err
	}

	var baseStorage storage.Storer = filesystem.NewStorage(baseFS, cache)
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
			baseFS:           baseFS,
			tmp:              tmp,
			tmpDir:           rootDir,
			transactional:    false,
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
	transactionStorage := filesystem.NewStorage(transactionFS, cache)

	var sto storage.Storer
	sto = transactional.NewStorage(baseStorage, transactionStorage)

	return &Storage{
		Storer:           sto,
		ReferenceStorage: refSto,
		base:             base,
		path:             path,
		baseFS:           baseFS,
		transFS:          transactionFS,
		tmp:              tmp,
		tmpDir:           rootDir,
		transactional:    true,
	}, nil
}

// Commit finishes the writes on a Storage. If transactional mode is enabled
// the backing transaction siva finishes writing and it is appended to the
// original siva file. If it's not transactional the original siva file is
// closed.
func (s *Storage) Commit() error {
	defer s.cleanup()

	if c, ok := s.Storer.(io.Closer); ok {
		err := c.Close()
		if err != nil {
			return err
		}
	}

	if err := s.PackRefs(); err != nil {
		return err
	}

	err := s.sync()
	if err != nil {
		return err
	}

	if !s.transactional {
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
	if c, ok := s.Storer.(io.Closer); ok {
		err := c.Close()
		if err != nil {
			return err
		}
	}

	if pErr := s.PackRefs(); pErr != nil {
		err = pErr
	}

	if sErr := s.sync(); sErr != nil {
		err = sErr
	}

	s.cleanup()
	return err
}

func (s *Storage) cleanup() error {
	return butil.RemoveAll(s.tmp, s.tmpDir)
}

func (s *Storage) filesystem() sivafs.SivaFS {
	var fs sivafs.SivaFS
	if s.transactional {
		fs = s.transFS
	} else {
		fs = s.baseFS
	}

	return fs
}

func (s *Storage) sync() error {
	if s.transactional && s.syncBase {
		if err := s.baseFS.Sync(); err != nil {
			return err
		}
	}

	return s.filesystem().Sync()
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
		err := s.baseFS.Remove(packedRefsPath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		if !os.IsNotExist(err) {
			s.syncBase = true
		}

		return nil
	}

	f, err := s.filesystem().OpenFile(
		packedRefsPath,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY,
		0660,
	)
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

	return s.removeRefsDir()
}

func (s *Storage) removeRefsDir() error {
	var needSync bool
	fs := s.filesystem()
	_, err := fs.Stat(refsPath)
	if err != nil {
		if fs == s.transFS {
			fs = s.baseFS
			_, err = fs.Stat(refsPath)

		}

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}

			if err := fs.MkdirAll(refsPath, 0770); err != nil {
				return err
			}

			needSync = true
		}
	}

	keepPath := filepath.Join(refsPath, keepFile)
	if _, err := fs.Stat(keepPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		f, err := fs.Create(keepPath)
		if err != nil {
			return err
		}

		if err := f.Close(); err != nil {
			return err
		}

		needSync = true
	}

	entries, err := fs.ReadDir(refsPath)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.Name() == keepFile && e.Mode().IsRegular() {
			continue
		}

		if err := butil.RemoveAll(
			fs,
			filepath.Join(refsPath, e.Name()),
		); err != nil {
			return err
		}

		needSync = true
	}

	if needSync {
		s.syncBase = true
	}

	return nil
}
