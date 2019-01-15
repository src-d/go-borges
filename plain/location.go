package plain

import (
	"io"
	"os"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/transactional"
)

type LocationOptions struct {
	Bare               bool
	Transactional      bool
	TemporalFilesystem billy.Filesystem
}

// Validate validates the fields and sets the default values.
func (o *LocationOptions) Validate() error {
	if o.Transactional && o.TemporalFilesystem == nil {
		o.TemporalFilesystem = memfs.New()
	}

	return nil
}

type Location struct {
	id   borges.LocationID
	fs   billy.Filesystem
	opts *LocationOptions
}

func NewLocation(id borges.LocationID, fs billy.Filesystem, opts *LocationOptions) (*Location, error) {
	if opts == nil {
		opts = &LocationOptions{}
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &Location{id: id, fs: fs, opts: opts}, nil
}

// ID returns the ID for this Location.
func (l *Location) ID() borges.LocationID {
	return l.id
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
		return nil, borges.ErrRepositoryExists.New(id)
	}

	s, err := l.repositoryStorer(id, borges.RWMode)
	if err != nil {
		return nil, err
	}

	r, err := borges.InitRepository(id, l.id, s)
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
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return l.doGet(id, mode)
}

// doGet, is the basic operation of open a repository without any checking.
func (l *Location) doGet(id borges.RepositoryID, mode borges.Mode) (*borges.Repository, error) {
	s, err := l.repositoryStorer(id, mode)
	if err != nil {
		return nil, err
	}

	return borges.OpenRepository(id, l.id, s)
}

func (l *Location) repositoryStorer(id borges.RepositoryID, mode borges.Mode) (
	storage.Storer, error) {

	fs, err := l.fs.Chroot(l.repositoryPath(id))
	if err != nil {
		return nil, err
	}

	s := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())

	switch mode {
	case borges.ReadOnlyMode:
		return &util.ReadOnlyStorer{s}, nil
	case borges.RWMode:
		if l.opts.Transactional {
			return l.repositoryTemporalStorer(id, s)
		}

		return s, nil
	default:
		return nil, borges.ErrModeNotSupported.New(mode)
	}
}

func (l *Location) repositoryTemporalStorer(id borges.RepositoryID, s storage.Storer) (
	storage.Storer, error) {

	fs, err := l.opts.TemporalFilesystem.Chroot(l.repositoryPath(id))
	if err != nil {
		return nil, err
	}

	ts := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	return transactional.NewStorage(s, ts), nil
}

func (l *Location) repositoryPath(id borges.RepositoryID) string {
	if l.opts.Bare {
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
		is, err := IsRepository(iter.l.fs, path, iter.l.opts.Bare)
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
