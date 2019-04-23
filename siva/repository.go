package siva

import (
	"sync"

	borges "github.com/src-d/go-borges"

	errors "gopkg.in/src-d/go-errors.v1"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage"
)

// ErrRepoAlreadyClosed is returned when a repository opened in RW mode was already closed.
var ErrRepoAlreadyClosed = errors.NewKind("repository % already closed")

// Repository is an implementation for siva files of borges.Repository
// interface.
type Repository struct {
	id            borges.RepositoryID
	repo          *git.Repository
	s             storage.Storer
	mode          borges.Mode
	transactional bool

	mu     sync.Mutex
	closed bool

	location *Location
}

var _ borges.Repository = (*Repository)(nil)

// newRepository creates a new siva backed Repository.
func newRepository(
	id borges.RepositoryID,
	sto storage.Storer,
	m borges.Mode,
	transactional bool,
	l *Location,
) (*Repository, error) {
	repo, err := git.Open(sto, nil)
	if err != nil {
		if err == git.ErrRepositoryNotExists {
			repo, err = git.Init(sto, nil)
		}

		if err != nil {
			return nil, borges.ErrLocationNotExists.Wrap(err, id)
		}
	}

	return &Repository{
		id:            id,
		repo:          repo,
		s:             sto,
		mode:          m,
		transactional: transactional,
		location:      l,
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRepoAlreadyClosed.New(r.id)
	}

	if !r.transactional {
		return borges.ErrNonTransactional.New()
	}

	defer func() { r.closed = true }()

	sto, ok := r.s.(Committer)
	if ok {
		err := sto.Commit()
		if err != nil {
			// TODO: log the rollback error
			_ = r.location.Rollback(r.mode)
			return err
		}
	}

	return r.location.Commit(r.mode)
}

// Close implements borges.Repository interface.
func (r *Repository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRepoAlreadyClosed.New(r.id)
	}
	defer func() { r.closed = true }()

	sto, ok := r.s.(Committer)
	if ok {
		err := sto.Close()
		if err != nil {
			// TODO: log rollback error
			_ = r.location.Rollback(r.mode)
			return err
		}
	}

	return r.location.Rollback(r.mode)
}

// R implements borges.Repository interface.
func (r *Repository) R() *git.Repository {
	return r.repo
}
