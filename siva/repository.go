package siva

import (
	borges "github.com/src-d/go-borges"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

// Repository is an implementation for siva files of borges.Repository
// interface.
type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	fs   sivafs.SivaFS
	mode borges.Mode

	location *Location
}

var _ borges.Repository = (*Repository)(nil)

// NewRepository creates a new siva backed Repository.
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

// ID implements borges.Repository interface.
func (r *Repository) ID() borges.RepositoryID {
	return r.id
}

// LocationID implements borges.Repository interface.
func (r *Repository) LocationID() borges.LocationID {
	return r.location.ID()
}

// Mode implements borges.Repository interface.
func (r *Repository) Mode() borges.Mode {
	return r.mode
}

// Commit implements borges.Repository interface.
func (r *Repository) Commit() error {
	if r.mode != borges.RWMode {
		return nil
	}

	err := r.fs.Sync()
	if err != nil {
		return err
	}

	return r.location.Commit(r.mode)
}

// Close implements borges.Repository interface.
func (r *Repository) Close() error {
	if r.mode != borges.RWMode {
		return nil
	}

	err := r.fs.Sync()
	if err != nil {
		return err
	}

	return r.location.Rollback(r.mode)
}

// R implements borges.Repository interface.
func (r *Repository) R() *git.Repository {
	return r.repo
}
