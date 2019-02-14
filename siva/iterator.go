package siva

import (
	"io"

	borges "github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4/config"
)

type repositoryIterator struct {
	mode    borges.Mode
	l       *Location
	pos     int
	remotes []*config.RemoteConfig
}

var _ borges.RepositoryIterator = (*repositoryIterator)(nil)

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

		fs, err := i.l.FS()
		if err != nil {
			return nil, err
		}

		id := toRepoID(r.URLs[0])
		return NewRepository(id, fs, i.mode, i.l)
	}
}

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

func (i *repositoryIterator) Close() {}
