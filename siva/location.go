package siva

import (
	"io"

	borges "github.com/src-d/go-borges"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
)

type Location struct {
	id   borges.LocationID
	repo *git.Repository
}

var _ borges.Location = (*Location)(nil)

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

		id := toRepoID(r.URLs[0])
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
