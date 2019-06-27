package oldsiva

import (
	"context"
	"io"

	"github.com/src-d/go-borges"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

// Location represents a siva file archiving several git repositories using an
// old rooted repository structure see
// https://github.com/src-d/borges/blob/master/docs/using-borges/key-concepts.md#rooted-repository.
type Location struct {
	id   borges.LocationID
	path string
	lib  *Library
}

var _ borges.Location = (*Location)(nil)

func newLocation(
	id borges.LocationID,
	lib *Library,
	path string,
) (*Location, error) {
	_, err := lib.fs.Stat(path)
	if err != nil {
		return nil, err
	}

	loc := &Location{
		id:   id,
		path: path,
		lib:  lib,
	}

	return loc, nil
}

// ID implements the borges.Location interface.
func (l *Location) ID() borges.LocationID {
	return l.id
}

// Library implements the borges.Location interface.
func (l *Location) Library() borges.Library {
	return l.lib
}

// Init implementes the borges.Location interface.
func (l *Location) Init(context.Context, borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Get implementes the borges.Location interface. It only retrieves repositories
// in borges.ReadOnlyMode ignoring the given parameter.
func (l *Location) Get(
	ctx context.Context,
	id borges.RepositoryID,
	_ borges.Mode,
) (borges.Repository, error) {
	if id != "" && string(id) != string(l.id) {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	repoFS, err := sivafs.NewFilesystemWithOptions(
		l.lib.fs, l.path, memfs.New(),
		sivafs.SivaFSOptions{
			UnsafePaths: true,
			ReadOnly:    true,
		},
	)

	if err != nil {
		return nil, err
	}

	repoCache := l.lib.opts.Cache
	if repoCache == nil {
		repoCache = cache.NewObjectLRUDefault()
	}

	return newRepository(l, repoFS, repoCache)
}

// GetOrInit implementes the borges.Location interface.
func (l *Location) GetOrInit(
	context.Context,
	borges.RepositoryID,
) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has implementes the borges.Location interface.
func (l *Location) Has(
	_ context.Context,
	id borges.RepositoryID,
) (bool, error) {
	return string(id) == string(l.id), nil
}

// Repositories implementes the borges.Location interface. It only retrieves
// repositories in borges.ReadOnlyMode ignoring the given parameter.
func (l *Location) Repositories(
	_ context.Context,
	_ borges.Mode,
) (borges.RepositoryIterator, error) {
	return &repoIter{loc: l}, nil
}

type repoIter struct {
	loc      *Location
	consumed bool
}

func (i *repoIter) Next() (borges.Repository, error) {
	if i.consumed {
		return nil, io.EOF
	}

	i.consumed = true
	id := borges.RepositoryID(i.loc.id)
	return i.loc.Get(context.TODO(), id, borges.ReadOnlyMode)
}

func (i *repoIter) ForEach(f func(borges.Repository) error) error {
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

func (i *repoIter) Close() {}
