package siva

import (
	"time"

	borges "github.com/src-d/go-borges"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
)

var (
	// ErrMalformedData when checkpoint data is invalid.
	ErrMalformedData = errors.NewKind("malformed data")
	// ErrTransactionTimeout is returned when a repository can't
	// be retrieved in transactional mode because of a timeout.
	ErrTransactionTimeout = errors.NewKind("timeout exceeded: unable toretrieve repository %s in transactional mode.")
)

type Location struct {
	id       borges.LocationID
	path     string
	cachedFS sivafs.SivaFS
	library  *Library

	// last good position
	checkpoint *Checkpoint
	tx         *repoTxer
}

var _ borges.Location = (*Location)(nil)

const txTimeout = 60 * time.Second

type repoTxer struct {
	notification chan struct{}
	timeout      time.Duration
}

func newRepoTxer() *repoTxer {
	n := make(chan struct{}, 1)
	n <- struct{}{}
	return &repoTxer{n, txTimeout}
}

func (rtx *repoTxer) Start(id borges.RepositoryID) error {
	select {
	case <-rtx.notification:
		return nil
	case <-time.After(rtx.timeout):
		return ErrTransactionTimeout.New(id)
	}
}

func (rtx *repoTxer) Stop() { rtx.notification <- struct{}{} }

func NewLocation(id borges.LocationID, l *Library, path string) (*Location, error) {
	checkpoint, err := NewCheckpoint(l.fs, path)
	if err != nil {
		return nil, err
	}

	location := &Location{
		id:         id,
		path:       path,
		library:    l,
		checkpoint: checkpoint,
	}

	_, err = location.FS()
	if err != nil {
		return nil, err
	}

	location.tx = newRepoTxer()
	return location, nil
}

func (l *Location) newFS() (sivafs.SivaFS, error) {
	return sivafs.NewFilesystem(l.baseFS(), l.path, memfs.New())
}

// FS returns a filesystem for the location's siva file.
func (l *Location) FS() (sivafs.SivaFS, error) {
	if l.cachedFS != nil {
		return l.cachedFS, nil
	}

	if err := l.checkpoint.Apply(); err != nil {
		return nil, err
	}

	sfs, err := l.newFS()
	if err != nil {
		return nil, err
	}

	l.cachedFS = sfs
	return sfs, nil
}

func (l *Location) ID() borges.LocationID {
	return l.id
}

func (l *Location) Init(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}
	if has {
		return nil, borges.ErrRepositoryExists.New(id)
	}

	fs, err := l.FS()
	if err != nil {
		return nil, err
	}

	repo, err := NewRepository(id, fs, borges.RWMode, l)
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

func (l *Location) Has(name borges.RepositoryID) (bool, error) {
	repo, err := l.repository("", borges.ReadOnlyMode)
	if err != nil {
		return false, err
	}
	config, err := repo.R().Config()
	if err != nil {
		return false, err
	}

	for _, r := range config.Remotes {
		if len(r.URLs) > 0 {
			id := toRepoID(r.URLs[0])
			if id == name {
				return true, nil
			}
		}
	}

	return false, nil
}

func (l *Location) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	var remotes []*config.RemoteConfig

	repo, err := l.repository("", borges.ReadOnlyMode)
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
		l:       l,
		pos:     0,
		remotes: remotes,
	}, nil
}

func (l *Location) baseFS() billy.Filesystem {
	return l.library.fs
}

func (l *Location) getRepoFs(id borges.RepositoryID, mode borges.Mode) (sivafs.SivaFS, error) {
	if !l.library.transactional || mode != borges.RWMode {
		return l.FS()
	}

	if err := l.tx.Start(id); err != nil {
		return nil, err
	}

	fs, err := l.newFS()
	if err != nil {
		return nil, err
	}

	if err := l.checkpoint.Save(); err != nil {
		return nil, err
	}

	return fs, nil
}

func (l *Location) Commit(mode borges.Mode) error {
	if !l.library.transactional || mode != borges.RWMode {
		return nil
	}

	defer l.tx.Stop()
	if err := l.checkpoint.Reset(); err != nil {
		return err
	}

	l.cachedFS = nil
	return nil
}

func (l *Location) Rollback(mode borges.Mode) error {
	if !l.library.transactional || mode != borges.RWMode {
		return nil
	}

	defer l.tx.Stop()
	if err := l.checkpoint.Apply(); err != nil {
		return err
	}

	l.cachedFS = nil
	return nil
}

func (l *Location) repository(
	id borges.RepositoryID,
	mode borges.Mode,
) (borges.Repository, error) {
	fs, err := l.getRepoFs(id, mode)
	if err != nil {
		return nil, err
	}

	return NewRepository(id, fs, mode, l)
}
