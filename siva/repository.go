package siva

import (
	borges "github.com/src-d/go-borges"
	git "gopkg.in/src-d/go-git.v4"
)

var _ borges.Repository = new(Repository)

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

type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	mode borges.Mode

	location *Location
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
	panic("not implemented")
}

func (r *Repository) Close() error {
	panic("not implemented")
}

func (r *Repository) R() *git.Repository {
	return r.repo
}
