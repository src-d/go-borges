package siva

import (
	borges "github.com/src-d/go-borges"
	git "gopkg.in/src-d/go-git.v4"
)

type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	mode borges.Mode

	location *Location
}

var _ borges.Repository = (*Repository)(nil)

func NewRepository(
	id borges.RepositoryID,
	r *git.Repository,
	m borges.Mode,
	l *Location,
) *Repository {
	return &Repository{
		id:       id,
		repo:     r,
		mode:     m,
		location: l,
	}
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
