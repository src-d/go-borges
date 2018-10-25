package plain

import (
	"io"
	"os"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var (
	ErrRepositoryExists    = errors.NewKind("repository %s already exists")
	ErrRepositoryNotExists = errors.NewKind("repository %s not exists")
)

// Library controls the persistence of multiple git repositories.
type Location struct {
	fs   billy.Filesystem
	bare bool
}

// NewLibrary creates a new Library based on the given filesystem.
func NewLocation(fs billy.Filesystem, bare bool) *Location {
	return &Location{fs: fs, bare: bare}
}

// GetOrInit get the requested repository based on the given URL, or inits a
// new repository. If the repository is opened this will be done in RWMode.
func (l *Location) GetOrInit(id borges.RepositoryID) (*borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return l.Get(id, borges.RWMode)
	}

	return l.Init(id)
}

// Init inits a new repository for the given URL.
func (l *Location) Init(id borges.RepositoryID) (*borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return nil, ErrRepositoryExists.New(id)
	}

	s, err := l.repositoryStorer(id)
	if err != nil {
		return nil, err
	}

	r, err := borges.InitRepository(id, s, nil)
	if err != nil {
		return nil, err
	}

	_, err = r.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{id.String()},
	})

	if err != nil {
		return nil, err
	}

	return r, nil
}

// Has returns true if a repository with the given URL exists.
func (l *Location) Has(id borges.RepositoryID) (bool, error) {
	_, err := l.fs.Stat(l.repositoryPath(id))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// Get get the requested repository based on the given URL.
func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (*borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, ErrRepositoryNotExists.New(id)
	}

	return l.doGet(id, mode)
}

// doGet, is the basic operation of open a repository without any checking.
func (l *Location) doGet(id borges.RepositoryID, mode borges.Mode) (*borges.Repository, error) {
	s, err := l.repositoryStorer(id)
	if err != nil {
		return nil, err
	}

	if mode == borges.ReadOnlyMode {
		s = &util.ReadOnlyStorer{s}
	}

	return borges.OpenRepository(id, s, nil)
}

func (l *Location) repositoryStorer(id borges.RepositoryID) (
	storage.Storer, error) {
	fs, err := l.fs.Chroot(l.repositoryPath(id))
	if err != nil {
		return nil, err
	}

	return filesystem.NewStorage(fs, cache.NewObjectLRUDefault()), nil
}

func (l *Location) repositoryPath(id borges.RepositoryID) string {
	if l.bare {
		return id.String()
	}

	return l.fs.Join(id.String(), ".git")
}

func (l *Location) Repositories(m borges.Mode) (borges.RepositoryIterator, error) {
	return NewLocationIterator(l, m)
}

type dir struct {
	path    string
	entries []os.FileInfo
}

type LocationIterator struct {
	l     *Location
	m     borges.Mode
	queue []*dir
}

func NewLocationIterator(l *Location, m borges.Mode) (*LocationIterator, error) {
	iter := &LocationIterator{l: l, m: m}
	return iter, iter.addDir("")
}

func (iter *LocationIterator) addDir(path string) error {
	entries, err := iter.l.fs.ReadDir(path)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	iter.queue = append([]*dir{{path: path, entries: entries}}, iter.queue...)
	return nil
}

func (iter *LocationIterator) nextRepositoryPath() (string, error) {
	var fi os.FileInfo
	for {
		if len(iter.queue) == 0 {
			return "", io.EOF
		}

		dir := iter.queue[0]
		fi, dir.entries = dir.entries[0], iter.queue[0].entries[1:]
		if len(dir.entries) == 0 {
			iter.queue = iter.queue[1:]
		}

		if !fi.IsDir() {
			continue
		}

		path := iter.l.fs.Join(dir.path, fi.Name())
		is, err := IsRepository(iter.l.fs, path, iter.l.bare)
		if err != nil {
			return path, err
		}

		if is {
			return path, nil
		}

		if err = iter.addDir(path); err != nil {
			return path, err
		}

		continue
	}
}

func (iter *LocationIterator) Next() (*borges.Repository, error) {
	path, err := iter.nextRepositoryPath()
	if err != nil {
		return nil, err
	}

	id := borges.RepositoryID(path)
	if err != nil {
		return nil, err
	}

	return iter.l.doGet(id, iter.m)

}

func (iter *LocationIterator) ForEach(cb func(*borges.Repository) error) error {
	return borges.ForEachIterator(iter, cb)
}

func (iter *LocationIterator) Close() {

}
