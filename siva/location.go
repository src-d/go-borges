package siva

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	borges "github.com/src-d/go-borges"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

var (
	// ErrMalformedData when checkpoint data is invalid.
	ErrMalformedData = errors.NewKind("malformed data")

	// ErrInvalidSize means that the siva size could not be correctly
	// retrieved.
	ErrInvalidSize = errors.NewKind("invalid siva size")
)

// Location represents a siva file archiving several git repositories.
type Location struct {
	id         borges.LocationID
	path       string
	lib        *Library
	checkpoint *checkpoint
	txer       *transactioner
	metadata   *locationMetadata

	// references and config cache
	refs    memory.ReferenceStorage
	config  *config.Config
	fSize   int64
	fTime   time.Time
	version int

	m sync.RWMutex
}

var _ borges.Location = (*Location)(nil)

// newLocation creates a new Location struct. If create is true and the siva
// file does not exist a new siva file is created.
func newLocation(
	id borges.LocationID,
	lib *Library,
	path string,
	create bool,
) (*Location, error) {
	var metadata *locationMetadata
	if lib.metadata != nil {
		var err error
		metadata, err = loadOrCreateLocationMetadata(lib.fs, string(id))
		if err != nil {
			// TODO: skip metadata if corrupted? log a warning?
			return nil, err
		}
	}

	cp, err := newCheckpoint(lib.fs, path, create)
	if err != nil {
		return nil, err
	}

	loc := &Location{
		id:         id,
		path:       path,
		lib:        lib,
		checkpoint: cp,
		metadata:   metadata,
		version:    -1,
	}

	loc.txer = newTransactioner(
		loc,
		lib.locReg,
		lib.options.TransactionTimeout,
	)

	return loc, nil
}

func (l *Location) checkAndUpdate() error {
	l.m.Lock()
	defer l.m.Unlock()

	stat, err := l.lib.fs.Stat(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	cp, err := newCheckpoint(l.lib.fs, l.path, false)
	if err != nil {
		return err
	}

	version, err := l.lib.Version()
	if err != nil {
		return err
	}

	if l.fSize == stat.Size() && l.fTime == stat.ModTime() &&
		l.version == version && l.checkpoint.Offset() == cp.Offset() {
		return nil
	}

	if cp.Offset() > 0 {
		err = l.updateCache(cp)
		if err != nil {
			return err
		}
	}

	l.checkpoint = cp
	l.fSize = stat.Size()
	l.fTime = stat.ModTime()
	l.version = version

	return nil
}

func (l *Location) updateCache(cp *checkpoint) error {
	fs, err := l.fs(borges.ReadOnlyMode, cp)
	if err != nil {
		return err
	}
	defer fs.Sync()

	var sto storage.Storer
	sto = filesystem.NewStorage(fs, l.cache())
	refIter, err := sto.IterReferences()
	if err != nil {
		return err
	}

	refSto, err := NewRefStorage(refIter)
	if err != nil {
		return err
	}
	l.refs = refSto

	c, err := sto.Config()
	if err != nil {
		return err
	}
	l.config = c

	return nil
}

// FS returns a filesystem for the location's siva file.
func (l *Location) FS(mode borges.Mode) (sivafs.SivaFS, error) {
	err := l.checkAndUpdate()
	if err != nil {
		return nil, err
	}

	l.m.RLock()
	checkpoint := l.checkpoint
	l.m.RUnlock()

	return l.fs(mode, checkpoint)
}

func (l *Location) fs(mode borges.Mode, cp *checkpoint) (sivafs.SivaFS, error) {
	if mode == borges.ReadOnlyMode {
		offset := cp.Offset()
		if offset == 0 {
			return nil, borges.ErrLocationNotExists.New(string(l.id))
		}

		if l.lib.metadata != nil {
			version, err := l.lib.Version()
			if err != nil {
				return nil, err
			}

			o, err := l.metadata.offset(version)
			if err != nil {
				return nil, err
			}

			if o > 0 {
				offset = o
			}
		}

		return sivafs.NewFilesystemWithOptions(
			l.lib.fs, l.path, memfs.New(),
			sivafs.SivaFSOptions{
				UnsafePaths: true,
				ReadOnly:    true,
				Offset:      offset,
			},
		)
	}

	if err := cp.Apply(); err != nil {
		return nil, err
	}

	sfs, err := sivafs.NewFilesystemWithOptions(
		l.lib.fs, l.path, memfs.New(),
		sivafs.SivaFSOptions{
			UnsafePaths: true,
			ReadOnly:    mode == borges.ReadOnlyMode,
		},
	)
	if err != nil {
		return nil, err
	}

	return sfs, nil
}

// ID implements the borges.Location interface.
func (l *Location) ID() borges.LocationID {
	return l.id
}

// Library implements the borges.Location interface.
func (l *Location) Library() borges.Library {
	return l.lib
}

const (
	urlSchema       = "git://%s.git"
	fetchHEADStr    = "+HEAD:refs/remotes/%s/HEAD"
	fetchRefSpecStr = "+refs/*:refs/remotes/%s/*"
)

// Init implements the borges.Location interface.
func (l *Location) Init(id borges.RepositoryID) (borges.Repository, error) {
	id = toRepoID(id.String())

	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return nil, borges.ErrRepositoryExists.New(id)
	}

	repo, err := l.repository(id, borges.RWMode)
	if err != nil {
		return nil, err
	}

	cfg := &config.RemoteConfig{
		Name: id.String(),
		URLs: []string{fmt.Sprintf(urlSchema, id.String())},
		Fetch: []config.RefSpec{
			config.RefSpec(fmt.Sprintf(fetchHEADStr, id)),
			config.RefSpec(fmt.Sprintf(fetchRefSpecStr, id)),
		},
	}

	_, err = repo.R().CreateRemote(cfg)
	if err != nil {
		return nil, err
	}

	remotes, err := repo.R().Remotes()
	if err != nil {
		return nil, err
	}

	if len(remotes) == 1 {
		c, err := repo.R().Config()
		if err != nil {
			return nil, err
		}

		c.Core.IsBare = true
		if err := repo.R().Storer.SetConfig(c); err != nil {
			return nil, err
		}
	}

	return repo, nil
}

// Get implements the borges.Location interface.
func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	if id == "" {
		return l.repository(id, mode)
	}

	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return l.repository(id, mode)
}

// GetOrInit implements the borges.Location interface.
func (l *Location) GetOrInit(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return l.repository(id, borges.RWMode)
	}

	return l.Init(id)
}

// Has implements the borges.Location interface.
func (l *Location) Has(repoID borges.RepositoryID) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		l.lib.options.Timeout,
	)
	defer cancel()

	return l.has(ctx, repoID)
}

func (l *Location) has(
	ctx context.Context,
	repoID borges.RepositoryID,
) (bool, error) {
	if l.lib.options.Transactional {
		l.m.RLock()
		offsetZero := l.checkpoint.Offset() == 0
		l.m.RUnlock()

		if offsetZero {
			return false, nil
		}
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	repo, err := l.repository("", borges.ReadOnlyMode)
	if err != nil {
		// the repository is still not initialized
		if borges.ErrLocationNotExists.Is(err) {
			return false, nil
		}
		return false, err
	}
	defer repo.Close()

	config, err := repo.R().Config()
	if err != nil {
		return false, err
	}

	name := toRepoID(repoID.String())
	for _, r := range config.Remotes {
		id := toRepoID(r.Name)
		if id == name {
			return true, nil
		}
		for _, url := range r.URLs {
			id = toRepoID(url)
			if id == name {
				return true, nil
			}
		}
	}

	return false, nil
}

// Repositories implements the borges.Location interface.
func (l *Location) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		l.lib.options.Timeout,
	)
	defer cancel()

	return l.repositories(ctx, mode)
}

func (l *Location) repositories(
	ctx context.Context,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	var remotes []*config.RemoteConfig

	// Return false when the siva file does not exist. If repository is
	// called it will create a new siva file.
	_, err := l.lib.fs.Stat(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return &repositoryIterator{
				mode:    mode,
				loc:     l,
				pos:     0,
				remotes: remotes,
			}, nil
		}
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	repo, err := l.repository("", borges.ReadOnlyMode)
	if borges.ErrLocationNotExists.Is(err) {
		return &repositoryIterator{
			mode:    mode,
			loc:     l,
			pos:     0,
			remotes: nil,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	defer repo.Close()

	cfg, err := repo.R().Config()
	if err != nil {
		return nil, err
	}

	for _, r := range cfg.Remotes {
		remotes = append(remotes, r)
	}

	return &repositoryIterator{
		mode:    mode,
		loc:     l,
		pos:     0,
		remotes: remotes,
	}, nil
}

// Commit persists transactional or write operations performed on the repositories.
func (l *Location) Commit(mode borges.Mode) error {
	if !l.lib.options.Transactional {
		return borges.ErrNonTransactional.New()
	}

	if mode != borges.RWMode {
		return nil
	}

	defer l.txer.Stop()
	l.m.RLock()
	defer l.m.RUnlock()
	if err := l.checkpoint.Reset(); err != nil {
		return err
	}

	return nil
}

// Rollback discard transactional or write operations performed on the repositories.
func (l *Location) Rollback(mode borges.Mode) error {
	if !l.lib.options.Transactional || mode != borges.RWMode {
		return nil
	}

	defer l.txer.Stop()
	l.m.RLock()
	defer l.m.RUnlock()
	if err := l.checkpoint.Apply(); err != nil {
		return err
	}

	return nil
}

func (l *Location) cache() cache.Object {
	if l.lib.options.Cache != nil {
		return l.lib.options.Cache
	}

	return cache.NewObjectLRUDefault()
}

func (l *Location) repository(
	id borges.RepositoryID,
	mode borges.Mode,
) (borges.Repository, error) {
	var sto storage.Storer
	var fs billy.Filesystem

	err := l.checkAndUpdate()
	if err != nil {
		return nil, err
	}

	switch mode {
	case borges.ReadOnlyMode:
		var err error

		l.m.RLock()
		defer l.m.RUnlock()
		fs, err = l.fs(mode, l.checkpoint)
		if err != nil {
			return nil, err
		}

		gitStorerOptions := filesystem.Options{}
		if l.lib.options.Performance {
			gitStorerOptions = filesystem.Options{
				ExclusiveAccess: true,
				KeepDescriptors: true,
			}
		}

		sync := fs.(sivafs.SivaSync)
		sto = filesystem.NewStorageWithOptions(fs, l.cache(), gitStorerOptions)
		sto, err = NewReadOnlyStorerInitialized(sto, sync, l.refs, l.config)
		if err != nil {
			return nil, err
		}
	case borges.RWMode:
		if l.lib.options.Transactional {
			if err := l.txer.Start(); err != nil {
				return nil, err
			}

			l.m.RLock()
			if err := l.checkpoint.Save(); err != nil {
				l.m.RUnlock()
				return nil, err
			}
			l.m.RUnlock()
		}

		sivaSto, err := NewStorage(l.lib.fs, l.path, l.lib.tmp,
			l.lib.options.Transactional, l.cache())
		if err != nil {
			if l.lib.options.Transactional {
				l.txer.Stop()
			}

			return nil, err
		}

		fs = sivaSto.filesystem()
		sto = sivaSto

	default:
		return nil, borges.ErrModeNotSupported.New(mode)
	}

	if id != "" && l.lib.options.RootedRepo {
		sto = NewRootedStorage(sto, string(id))
	}

	return newRepository(id, sto, fs, mode, l.lib.options.Transactional, l)
}

// LastVersion returns the last defined version number in metadata or -1 if
// there are not versions.
func (l *Location) LastVersion() int {
	return l.metadata.last()
}

// Version returns an specific version. If the given version does not exist
// an error is returned.
func (l *Location) Version(v int) (*Version, error) {
	if l.lib.metadata != nil {
		return l.metadata.version(v)
	}

	return nil, errLocVersionNotExists.New()
}

// SetVersion adds or changes a version to the location.
func (l *Location) SetVersion(n int, v *Version) {
	if l.lib.metadata != nil {
		l.metadata.setVersion(n, v)
	}
}

// DeleteVersion removes the given version number.
func (l *Location) DeleteVersion(n int) {
	if l.lib.metadata != nil {
		l.metadata.deleteVersion(n)
	}
}

// SaveMetadata writes the location metadata to disk.
func (l *Location) SaveMetadata() error {
	if l.metadata != nil {
		return l.metadata.save()
	}

	return nil
}

func (l *Location) size() (uint64, error) {
	l.m.RLock()
	defer l.m.RUnlock()

	stat, err := l.lib.fs.Stat(l.path)
	if err != nil {
		return 0, err
	}

	size := stat.Size()
	if size < 0 {
		return 0, ErrInvalidSize.New()
	}

	return uint64(stat.Size()), nil
}
