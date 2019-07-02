package legacysiva

import (
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

// Repository is an implementation of borges.Repository
// interface based on siva files archiving rooted repositories using an old
// structure. See
// https://github.com/src-d/borges/blob/master/docs/using-borges/key-concepts.md#rooted-repository.
// It only supports read operations on the repositories and it doesn't support
// transactionality.
type Repository struct {
	id   borges.RepositoryID
	loc  *Location
	repo *git.Repository
	sto  *siva.ReadOnlyStorer
	fs   billy.Filesystem
}

var _ borges.Repository = (*Repository)(nil)

func newRepository(
	location *Location,
) (*Repository, error) {
	fs, err := location.fs()
	if err != nil {
		return nil, err
	}

	sto := filesystem.NewStorageWithOptions(
		fs,
		location.cache(),
		filesystem.Options{
			ExclusiveAccess: true,
			KeepDescriptors: true,
		},
	)

	roSto, err := siva.NewReadOnlyStorerInitialized(
		sto,
		fs.(sivafs.SivaSync),
		location.refs,
		location.config,
	)
	if err != nil {
		return nil, err
	}

	repo, err := git.Open(roSto, nil)
	if err != nil {
		return nil, err
	}

	return &Repository{
		id:   borges.RepositoryID(location.ID()),
		loc:  location,
		repo: repo,
		sto:  roSto,
		fs:   fs,
	}, nil
}

// ID implements the borges.Repository interface.
func (r *Repository) ID() borges.RepositoryID {
	return r.id
}

// Location implements the borges.Repository interface.
func (r *Repository) Location() borges.Location {
	return r.loc
}

// Mode implements the borges.Repository interface. It always
// returns borges.ReadOnlyMode.
func (r *Repository) Mode() borges.Mode {
	return borges.ReadOnlyMode
}

// Commit implements the borges.Repository interface. It always returns an
// borges.ErrNonTransactional error.
func (r *Repository) Commit() error {
	return borges.ErrNonTransactional.New()
}

// Close implements the borges.Repository interface.
func (r *Repository) Close() error {
	return r.sto.Close()
}

// R implements the borges.Repository interface.
func (r *Repository) R() *git.Repository {
	return r.repo
}

// FS implements the borges.Repository interface.
func (r *Repository) FS() billy.Filesystem {
	return r.fs
}
