package siva

import (
	"os"

	borges "github.com/src-d/go-borges"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

// ErrMalformedData when checkpoint data is invalid.
var ErrMalformedData = errors.NewKind("malformed data")

// Location represents a siva file archiving several git repositories.
type Location struct {
	id         borges.LocationID
	path       string
	lib        *Library
	checkpoint *checkpoint
	txer       *transactioner
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
	cp, err := newCheckpoint(lib.fs, path, create)
	if err != nil {
		return nil, err
	}

	loc := &Location{
		id:         id,
		path:       path,
		lib:        lib,
		checkpoint: cp,
	}

	loc.txer = newTransactioner(loc, lib.locReg, lib.timeout)
	return loc, nil
}

// FS returns a filesystem for the location's siva file.
func (l *Location) FS(mode borges.Mode) (sivafs.SivaFS, error) {
	if mode == borges.ReadOnlyMode {
		return sivafs.NewFilesystemWithOptions(
			l.lib.fs, l.path, memfs.New(),
			sivafs.SivaFSOptions{
				UnsafePaths: true,
				ReadOnly:    true,
				Offset:      l.checkpoint.Offset(),
			},
		)
	}

	if err := l.checkpoint.Apply(); err != nil {
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
		URLs: []string{id.String()},
	}

	_, err = repo.R().CreateRemote(cfg)
	if err != nil {
		return nil, err
	}

	return repo, nil
}

// Get implements the borges.Location interface.
func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
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
	// Return false when the siva file does not exist. If repository is
	// called it will create a new siva file.
	_, err := l.lib.fs.Stat(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	repo, err := l.repository("", borges.ReadOnlyMode)
	if err != nil {
		// the repository is still not initialized
		if borges.ErrLocationNotExists.Is(err) {
			return false, nil
		}
		return false, err
	}

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
	if !l.lib.transactional {
		return borges.ErrNonTransactional.New()
	}

	if mode != borges.RWMode {
		return nil
	}

	defer l.txer.Stop()
	if err := l.checkpoint.Reset(); err != nil {
		return err
	}

	return nil
}

// Rollback discard transactional or write operations performed on the repositories.
func (l *Location) Rollback(mode borges.Mode) error {
	if !l.lib.transactional || mode != borges.RWMode {
		return nil
	}

	defer l.txer.Stop()
	if err := l.checkpoint.Apply(); err != nil {
		return err
	}

	return nil
}

func (l *Location) repository(
	id borges.RepositoryID,
	mode borges.Mode,
) (borges.Repository, error) {
	var sto storage.Storer
	switch mode {
	case borges.ReadOnlyMode:
		fs, err := l.FS(mode)
		if err != nil {
			return nil, err
		}

		sto = filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
		sto = NewReadOnlyStorer(sto)
		if id != "" && l.lib.rooted {
			sto = NewRootedStorage(sto, string(id))
		}
	case borges.RWMode:
		if l.lib.transactional {
			if err := l.txer.Start(); err != nil {
				return nil, err
			}

			if err := l.checkpoint.Save(); err != nil {
				return nil, err
			}
		}

		var err error
		sto, err = NewStorage(l.lib.fs, l.path, l.lib.tmp, l.lib.transactional, "")
		if err != nil {
			if l.lib.transactional {
				l.txer.Stop()
			}

			return nil, err
		}

		if id != "" && l.lib.rooted {
			sto = NewRootedStorage(sto, string(id))
		}

	default:
		return nil, borges.ErrModeNotSupported.New(mode)
	}

	return newRepository(id, sto, mode, l.lib.transactional, l)
}
