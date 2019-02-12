package siva

import (
	"io"
	"os"

	borges "github.com/src-d/go-borges"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var _ borges.Location = new(Location)

func NewLocation(
	id borges.LocationID,
	fs billy.Filesystem,
	path string,
) (*Location, error) {
	_, err := fs.Stat(path)
	if os.IsNotExist(err) {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	sfs, err := sivafs.NewFilesystem(fs, path, memfs.New())
	if err != nil {
		return nil, err
	}

	sto := filesystem.NewStorage(sfs, cache.NewObjectLRUDefault())
	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return &Location{id: id, repo: repo}, nil
}

type Location struct {
	id   borges.LocationID
	repo *git.Repository
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

	cfg := &config.RemoteConfig{
		Name: id.String(),
		URLs: []string{id.String()},
	}

	_, err = l.repo.CreateRemote(cfg)
	if err != nil {
		return nil, err
	}

	return NewRepository(id, l.repo, borges.RWMode, l), nil
}

func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return l.repository(id, mode), nil
}

func (l *Location) GetOrInit(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return l.repository(id, borges.RWMode), nil
	}

	return l.Init(id)
}

func (l *Location) Has(name borges.RepositoryID) (bool, error) {
	config, err := l.repo.Config()
	if err != nil {
		return false, err
	}

	for _, r := range config.Remotes {
		if len(r.URLs) > 0 {
			id := repoID(r.URLs[0])
			if id == name {
				return true, nil
			}
		}
	}

	return false, nil
}

func (l *Location) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	var remotes []*config.RemoteConfig

	cfg, err := l.repo.Config()
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

func (l *Location) repository(
	id borges.RepositoryID,
	mode borges.Mode,
) borges.Repository {
	return NewRepository(id, l.repo, mode, l)
}

type repositoryIterator struct {
	mode    borges.Mode
	l       *Location
	pos     int
	remotes []*config.RemoteConfig
}

func (i *repositoryIterator) Next() (borges.Repository, error) {
	for {
		if i.pos >= len(i.remotes) {
			return nil, io.EOF
		}

		r := i.remotes[i.pos]
		i.pos++

		if len(r.URLs) == 0 {
			continue
		}

		id := repoID(r.URLs[0])
		return NewRepository(id, i.l.repo, i.mode, i.l), nil
	}
}

func (i *repositoryIterator) Close() {}

func (i *repositoryIterator) ForEach(f func(borges.Repository) error) error {
	for {
		r, err := i.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		err = f(r)
		if err != nil {
			return err
		}
	}
}
