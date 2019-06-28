package siva

import (
	"io"
	"sync"

	borges "github.com/src-d/go-borges"

	billy "gopkg.in/src-d/go-billy.v4"
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
	fs            billy.Filesystem
	mode          borges.Mode
	transactional bool

	mu     sync.Mutex
	closed bool

	location      *Location
	createVersion int
}

var _ borges.Repository = (*Repository)(nil)

// newRepository creates a new siva backed Repository.
func newRepository(
	id borges.RepositoryID,
	sto storage.Storer,
	fs billy.Filesystem,
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
		fs:            fs,
		mode:          m,
		transactional: transactional,
		location:      l,
		createVersion: -1,
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

	err := r.saveVersion()
	if err != nil {
		return err
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

	sto, ok := r.s.(io.Closer)
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

// FS returns the filesystem to read or write directly to the repository or
// nil if not available.
func (r *Repository) FS() billy.Filesystem {
	return r.fs
}

// VersionOnCommit specifies the version that will be set when the changes
// are committed. Only works for transactional repositories.
func (r *Repository) VersionOnCommit(n int) {
	r.createVersion = n
}

func (r *Repository) saveVersion() error {
	if r.createVersion < 0 {
		return nil
	}

	offset, err := r.location.size()
	if err != nil {
		return err
	}

	size := offset + 1

	// work with metadata directly to get the offset of previous version
	metadata := r.location.metadata
	if metadata != nil {
		metadata.DeleteVersion(r.createVersion)
		previousOffset := metadata.Offset(r.createVersion)
		if previousOffset > 0 {
			size = offset - previousOffset
		}
	}

	r.location.SetVersion(r.createVersion, Version{
		Offset: offset,
		Size:   size,
	})

	return r.location.SaveMetadata()
}
