package plain

import (
	"io"
	"os"

	"github.com/src-d/go-borges"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
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
func (l *Location) GetOrInit(id borges.RepositoryID) (borges.Repository, error) {
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
func (l *Location) Init(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return nil, borges.ErrRepositoryExists.New(id)
	}

	return initRepository(l, id)
}

// Has returns true if a repository with the given URL exists.
func (l *Location) Has(id borges.RepositoryID) (bool, error) {
	_, err := l.fs.Stat(l.RepositoryPath(id))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// Get get the requested repository based on the given URL.
func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return openRepository(l, id, mode)
}

// RepositoryPath returns the location in the filesystem for a given RepositoryID.
func (l *Location) RepositoryPath(id borges.RepositoryID) string {
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

func (iter *LocationIterator) Next() (borges.Repository, error) {
	path, err := iter.nextRepositoryPath()
	if err != nil {
		return nil, err
	}

	id := borges.RepositoryID(path)
	if err != nil {
		return nil, err
	}

	return openRepository(iter.l, id, iter.m)

}

func (iter *LocationIterator) ForEach(cb func(borges.Repository) error) error {
	return borges.ForEachIterator(iter, cb)
}

func (iter *LocationIterator) Close() {

}
