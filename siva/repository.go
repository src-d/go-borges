package siva

import (
	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
	billy "gopkg.in/src-d/go-billy.v4"
	git "gopkg.in/src-d/go-git.v4"
)

type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	fs   billy.Filesystem
	mode borges.Mode

	location *Location
}

var _ borges.Repository = (*Repository)(nil)

func NewRepository(
	id borges.RepositoryID,
	fs billy.Filesystem,
	m borges.Mode,
	l *Location,
) (*Repository, error) {
	sto, _, err := util.RepositoryStorer(fs, l.library.fs, m, l.transactional)
	if err != nil {
		return nil, err
	}

	repo, err := git.Open(sto, nil)
	if err != nil {
		return nil, err
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
	return borges.ErrNotImplemented.New()
}

func (r *Repository) Close() error {
	return borges.ErrNotImplemented.New()
}

func (r *Repository) R() *git.Repository {
	return r.repo
}
