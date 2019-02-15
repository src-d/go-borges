package siva

import (
	borges "github.com/src-d/go-borges"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	fs   sivafs.SivaFS
	mode borges.Mode

	location *Location
}

var _ borges.Repository = (*Repository)(nil)

func NewRepository(
	id borges.RepositoryID,
	fs sivafs.SivaFS,
	m borges.Mode,
	l *Location,
) (*Repository, error) {
	sto := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return &Repository{
		id:       id,
		repo:     repo,
		fs:       fs,
		mode:     m,
		location: l,
	}, nil
}

func (r *Repository) ID() borges.RepositoryID {
	return r.id
}

func (r *Repository) LocationID() borges.LocationID {
	return r.location.ID()
}

func (r *Repository) Mode() borges.Mode {
	return r.mode
}

func (r *Repository) Commit() error {
	err := r.fs.Sync()
	if err != nil {
		return err
	}

	return r.location.Commit(r.mode)
}

func (r *Repository) Close() error {
	err := r.fs.Sync()
	if err != nil {
		return err
	}

	return r.location.Rollback(r.mode)
}

func (r *Repository) R() *git.Repository {
	return r.repo
}
