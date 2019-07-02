package legacysiva

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

// Location represents a siva file archiving several git repositories using an
// old rooted repository structure see
// https://github.com/src-d/borges/blob/master/docs/using-borges/key-concepts.md#rooted-repository.
type Location struct {
	id   borges.LocationID
	path string
	lib  *Library

	// references and config cache
	refs   memory.ReferenceStorage
	config *config.Config
	fSize  int64
	fTime  time.Time

	m sync.RWMutex
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

// Init implements the borges.Location interface.
func (l *Location) Init(_ borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Get implements the borges.Location interface. It only retrieves repositories
// in borges.ReadOnlyMode ignoring the given parameter.
func (l *Location) Get(
	id borges.RepositoryID, _ borges.Mode,
) (borges.Repository, error) {
	if id != "" && string(id) != string(l.id) {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	err := l.checkAndUpdate()
	if err != nil {
		return nil, err
	}

	return newRepository(l)
}

func (l *Location) cache() cache.Object {
	repoCache := l.lib.opts.Cache
	if repoCache == nil {
		repoCache = cache.NewObjectLRUDefault()
	}

	return repoCache
}

func (l *Location) fs() (sivafs.SivaFS, error) {
	return sivafs.NewFilesystemWithOptions(
		l.lib.fs, l.path, memfs.New(),
		sivafs.SivaFSOptions{
			UnsafePaths: true,
			ReadOnly:    true,
		},
	)
}

// GetOrInit implements the borges.Location interface.
func (l *Location) GetOrInit(_ borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has implements the borges.Location interface.
func (l *Location) Has(id borges.RepositoryID) (bool, error) {
	return string(id) == string(l.id), nil
}

// Repositories implements the borges.Location interface. It only retrieves
// repositories in borges.ReadOnlyMode ignoring the given parameter.
func (l *Location) Repositories(
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
	return i.loc.Get(id, borges.ReadOnlyMode)
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

func (l *Location) checkAndUpdate() error {
	l.m.Lock()
	defer l.m.Unlock()

	stat, err := l.lib.fs.Stat(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	if l.fSize == stat.Size() && l.fTime == stat.ModTime() {
		return nil
	}

	err = l.updateCache()
	if err != nil {
		return err
	}

	l.fSize = stat.Size()
	l.fTime = stat.ModTime()

	return nil
}

func (l *Location) updateCache() error {
	fs, err := l.fs()
	if err != nil {
		return err
	}
	defer fs.Sync()

	var sto storage.Storer
	sto = filesystem.NewStorage(fs, l.cache())
	refIter, err := sto.IterReferences()
	if err != nil {
		return err
	}

	refSto, err := siva.NewRefStorage(refIter)
	if err != nil {
		return err
	}
	l.refs = refSto

	c, err := sto.Config()
	if err != nil {
		return err
	}
	l.config = c

	return nil
}
