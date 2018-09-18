package plain

import (
	"fmt"
	"io"
	"os"

	"github.com/src-d/go-borges"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
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
	fs billy.Filesystem
}

// NewLibrary creates a new Library based on the given filesystem.
func NewLocation(fs billy.Filesystem) *Location {
	return &Location{fs: fs}
}

// GetOrInit get the requested repository based on the given URL, or inits a
// new repository.
func (l *Location) GetOrInit(id borges.RepositoryID) (*git.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return l.Get(id)
	}

	return l.Init(id)
}

// Init inits a new repository for the given URL.
func (l *Location) Init(id borges.RepositoryID) (*git.Repository, error) {
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

	r, err := git.Init(s, nil)
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
func (l *Location) Get(id borges.RepositoryID) (*git.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, ErrRepositoryNotExists.New(id)
	}

	s, err := l.repositoryStorer(id)
	if err != nil {
		return nil, err
	}

	return git.Open(s, nil)
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
	return id.String()
}

type dir struct {
	path    string
	entries []os.FileInfo
}

type LocationIterator struct {
	fs    billy.Filesystem
	queue []*dir
}

func NewLocationIterator(fs billy.Filesystem) (*LocationIterator, error) {
	iter := &LocationIterator{fs: fs}
	return iter, iter.addDir("")
}

func (iter *LocationIterator) addDir(path string) error {
	entries, err := iter.fs.ReadDir(path)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	iter.queue = append([]*dir{{path: path, entries: entries}}, iter.queue...)
	return nil
}

func (iter *LocationIterator) nextRepositoryPath() (path string, isBare bool, err error) {
	var fi os.FileInfo
	var is bool
	for {
		if len(iter.queue) == 0 {
			err = io.EOF
			return
		}

		dir := iter.queue[0]
		fi, dir.entries = dir.entries[0], iter.queue[0].entries[1:]
		if len(dir.entries) == 0 {
			iter.queue = iter.queue[1:]
		}

		if !fi.IsDir() {
			continue
		}

		path = iter.fs.Join(dir.path, fi.Name())
		is, isBare, err = IsRepository(iter.fs, path)
		if err != nil {
			return
		}

		if is {
			return
		}

		if err = iter.addDir(path); err != nil {
			return
		}

		continue
	}
}

func (iter *LocationIterator) Next() (borges.RepositoryID, *borges.Repository, error) {
	path, isBare, err := iter.nextRepositoryPath()
	if err != nil {
		return "", nil, err
	}

	id := borges.RepositoryID(path)
	if err != nil {
		return id, nil, err
	}

	fmt.Println(isBare)
	return id, nil, err

}

func (iter *LocationIterator) ForEach(cb func(borges.RepositoryID, *borges.Repository) error) error {
	return borges.ForEachIterator(iter, cb)
}

func (iter *LocationIterator) Close() {

}
