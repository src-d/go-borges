package siva

import (
	"fmt"
	"io"
	"os"
	"strconv"

	borges "github.com/src-d/go-borges"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/config"
)

var (
	// ErrCannotUseCheckpointFile is returned on checkpoint problems.
	ErrCannotUseCheckpointFile = errors.NewKind("cannot use checkpoint file: %s")
	// ErrCannotUseSivaFile is returned on siva problems.
	ErrCannotUseSivaFile = errors.NewKind("cannot use siva file: %s")
	// ErrMalformedData when checkpoint data is invalid.
	ErrMalformedData = errors.NewKind("malformed data")
)
var _ borges.Location = new(Location)

func NewLocation(
	id borges.LocationID,
	l *Library,
	path string,
) (*Location, error) {
	err := fixSiva(l.fs, path)
	if err != nil {
		return nil, err
	}

	_, err = l.fs.Stat(path)
	if os.IsNotExist(err) {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	location := &Location{
		id:      id,
		path:    path,
		library: l,
	}

	_, err = location.FS()
	if err != nil {
		return nil, err
	}

	return location, nil
}

type Location struct {
	id            borges.LocationID
	path          string
	cachedFS      billy.Filesystem
	transactional bool
	library       *Library
}

var _ borges.Location = (*Location)(nil)

// fixSiva searches for a file named path.checkpoint. If it's found it truncates
// the siva file to the size written in it.
func fixSiva(fs billy.Filesystem, path string) error {
	checkpointPath := fmt.Sprintf("%s.checkpoint", path)

	checkErr := func(err error) error {
		return ErrCannotUseCheckpointFile.Wrap(err, checkpointPath)
	}
	sivaErr := func(err error) error {
		return ErrCannotUseSivaFile.Wrap(err, path)
	}

	cf, err := fs.Open(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return checkErr(err)
	}
	defer cf.Close()

	// there's a checkpoint file we can use to fix the siva file

	sf, err := fs.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return sivaErr(err)
		}

		// there's checkpoint but not siva file, delete checkpoint
		err = cf.Close()
		if err != nil {
			return checkErr(err)
		}

		err = fs.Remove(checkpointPath)
		if err != nil {
			return checkErr(err)
		}

		return nil
	}
	defer sf.Close()

	// the biggest 64 bit number in decimal ASCII is 19 characters
	data := make([]byte, 32)
	n, err := cf.Read(data)
	if err != nil {
		return checkErr(err)
	}

	size, err := strconv.ParseInt(string(data[:n]), 10, 64)
	if err != nil {
		return checkErr(err)
	}
	if size < 0 {
		return checkErr(ErrMalformedData.New())
	}

	err = sf.Truncate(size)
	if err != nil {
		return sivaErr(err)
	}

	err = cf.Close()
	if err != nil {
		return checkErr(err)
	}

	err = fs.Remove(checkpointPath)
	if err != nil {
		return checkErr(err)
	}

	return nil
}

// FS returns a filesystem for the location's siva file.
func (l *Location) FS() (billy.Filesystem, error) {
	if l.cachedFS != nil {
		return l.cachedFS, nil
	}

	err := fixSiva(l.library.fs, l.path)
	if err != nil {
		return nil, err
	}

	sfs, err := sivafs.NewFilesystem(l.library.fs, l.path, memfs.New())
	if err != nil {
		return nil, err
	}

	l.cachedFS = sfs
	return sfs, nil
}

func (l *Location) ID() borges.LocationID {
	return l.id
}

func (l *Location) Init(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}
	if has {
		return nil, borges.ErrRepositoryExists.New(id)
	}

	fs, err := l.FS()
	if err != nil {
		return nil, err
	}

	repo, err := NewRepository(id, fs, borges.RWMode, l)
	if err != nil {
		return nil, err
	}

	cfg := &config.RemoteConfig{
		Name: id.String(),
		URLs: []string{id.String()},
	}

	_, err = repo.R().CreateRemote(cfg)
	if err != nil {
		return nil, err
	}

	return repo, nil
}

func (l *Location) Get(id borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	return l.repository(id, mode)
}

func (l *Location) GetOrInit(id borges.RepositoryID) (borges.Repository, error) {
	has, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if has {
		return l.repository(id, borges.RWMode)
	}

	return l.Init(id)
}

func (l *Location) Has(name borges.RepositoryID) (bool, error) {
	repo, err := l.repository("", borges.ReadOnlyMode)
	if err != nil {
		return false, err
	}
	config, err := repo.R().Config()
	if err != nil {
		return false, err
	}

	for _, r := range config.Remotes {
		if len(r.URLs) > 0 {
			id := toRepoID(r.URLs[0])
			if id == name {
				return true, nil
			}
		}
	}

	return false, nil
}

func (l *Location) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	var remotes []*config.RemoteConfig

	repo, err := l.repository("", borges.ReadOnlyMode)
	if err != nil {
		return nil, err
	}
	cfg, err := repo.R().Config()
	if err != nil {
		return nil, err
	}

	for _, r := range cfg.Remotes {
		remotes = append(remotes, r)
	}

	return &repositoryIterator{
		mode:    mode,
		l:       l,
		pos:     0,
		remotes: remotes,
	}, nil
}

func (l *Location) repository(
	id borges.RepositoryID,
	mode borges.Mode,
) (borges.Repository, error) {
	fs, err := l.FS()
	if err != nil {
		return nil, err
	}

	return NewRepository(id, fs, mode, l)
}

type repositoryIterator struct {
	mode    borges.Mode
	l       *Location
	pos     int
	remotes []*config.RemoteConfig
}

func (i *repositoryIterator) Next() (borges.Repository, error) {
	for {
		if i.pos >= len(i.remotes) {
			return nil, io.EOF
		}

		r := i.remotes[i.pos]
		i.pos++

		if len(r.URLs) == 0 {
			continue
		}

		fs, err := i.l.FS()
		if err != nil {
			return nil, err
		}

		id := toRepoID(r.URLs[0])
		return NewRepository(id, fs, i.mode, i.l)
	}
}

func (i *repositoryIterator) Close() {}

func (i *repositoryIterator) ForEach(f func(borges.Repository) error) error {
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
