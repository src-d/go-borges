package siva

import (
	"sync"

	borges "github.com/src-d/go-borges"

	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

// ErrReposArleadyClosed is returned when a repository opened in RW mode was already closed.
var ErrRepoAlreadyClosed = errors.NewKind("repository % already closed")

// Repository is an implementation for siva files of borges.Repository
// interface.
type Repository struct {
	id   borges.RepositoryID
	repo *git.Repository
	fs   sivafs.SivaFS
	mode borges.Mode

	mu     sync.Mutex
	closed bool

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
		if err == git.ErrRepositoryNotExists {
			repo, err = git.Init(sto, nil)
		}
		if err != nil {
			return nil, borges.ErrLocationNotExists.New(id)
		}
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

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRepoAlreadyClosed.New(r.id)
	}

	err := r.fs.Sync()
	if err != nil {
		return err
	}

	r.closed = true
	return r.location.Commit(r.mode)
}

// Close implements borges.Repository interface.
func (r *Repository) Close() error {
	if r.mode != borges.RWMode {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRepoAlreadyClosed.New(r.id)
	}

	err := r.fs.Sync()
	if err != nil {
		return err
	}

	r.closed = true
	return r.location.Rollback(r.mode)
}

// R implements borges.Repository interface.
func (r *Repository) R() *git.Repository {
	return r.repo
}
