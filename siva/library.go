package siva

import (
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	butil "gopkg.in/src-d/go-billy.v4/util"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
)

// ErrLocationExists when the location to be created already exists.
var ErrLocationExists = errors.NewKind("location %s already exists")

// Library represents a borges.Library implementation based on siva files.
type Library struct {
	id     borges.LibraryID
	fs     billy.Filesystem
	tmp    billy.Filesystem
	locReg *locationRegistry

	options  LibraryOptions
	metadata *LibraryMetadata
}

// LibraryOptions hold configuration options for the library.
type LibraryOptions struct {
	// Transactional enables transactions for repository writes.
	Transactional bool
	// Timeout is the time it will wait while another transaction
	// is being done before error. 0 means default.
	Timeout time.Duration
	// RegistryCache is the maximum number of locations in the cache. A value
	// of 0 disables the cache.
	RegistryCache int
	// TempFS is the temporary filesystem to do transactions and write files.
	TempFS billy.Filesystem
	// Bucket level to use to search and create siva files.
	Bucket int
	// RootedRepo makes the repository show only the references for the remote
	// named with the repository ID.
	RootedRepo bool
	// Cache specifies the shared cache used in repositories. If not defined
	// a new default cache will be created for each repository.
	Cache cache.Object
	// Performance enables performance options in read only git repositories
	// (ExclusiveAccess and KeepDescriptors).
	Performance bool
}

var _ borges.Library = (*Library)(nil)

// txTimeout is the default transaction timeout.
const txTimeout = 60 * time.Second

// NewLibrary creates a new siva.Library.
func NewLibrary(
	id string,
	fs billy.Filesystem,
	ops LibraryOptions,
) (*Library, error) {
	metadata, err := loadLibraryMetadata(fs)
	if err != nil {
		// TODO: skip metadata if corrupted?
		return nil, err
	}

	lr, err := newLocationRegistry(ops.RegistryCache)
	if err != nil {
		return nil, err
	}

	if ops.Timeout == 0 {
		ops.Timeout = txTimeout
	}

	tmp := ops.TempFS
	if tmp == nil {
		dir, err := ioutil.TempDir("", "go-borges")
		if err != nil {
			return nil, err
		}

		tmp = osfs.New(dir)
	}

	return &Library{
		id:       borges.LibraryID(id),
		fs:       fs,
		tmp:      tmp,
		locReg:   lr,
		options:  ops,
		metadata: metadata,
	}, nil
}

// ID implements borges.Library interface.
func (l *Library) ID() borges.LibraryID {
	return l.id
}

// Init implements borges.Library interface.
func (l *Library) Init(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Get implements borges.Library interface.
func (l *Library) Get(repoID borges.RepositoryID, mode borges.Mode) (borges.Repository, error) {
	ok, _, locID, err := l.Has(repoID)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, borges.ErrRepositoryNotExists.New(repoID)
	}

	loc, err := l.Location(locID)
	if err != nil {
		return nil, err
	}

	return loc.Get(repoID, mode)
}

// GetOrInit implements borges.Library interface.
func (l *Library) GetOrInit(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

func toRepoID(endpoint string) borges.RepositoryID {
	name, _ := borges.NewRepositoryID(endpoint)
	return borges.RepositoryID(strings.TrimSuffix(name.String(), ".git"))
}

func toLocID(file string) borges.LocationID {
	id := strings.TrimSuffix(file, ".siva")
	return borges.LocationID(id)
}

// Has implements borges.Library interface.
func (l *Library) Has(name borges.RepositoryID) (bool, borges.LibraryID, borges.LocationID, error) {
	it, err := l.Locations()
	if err != nil {
		return false, "", "", err
	}
	defer it.Close()

	for {
		loc, err := it.Next()
		if err == io.EOF {
			return false, "", "", nil
		}
		if err != nil {
			return false, "", "", err
		}

		has, err := loc.Has(name)
		if err != nil {
			return false, "", "", err
		}

		if has {
			return true, l.id, loc.ID(), nil
		}
	}
}

// Repositories implements borges.Library interface.
func (l *Library) Repositories(mode borges.Mode) (borges.RepositoryIterator, error) {
	locs, err := l.locations()
	if err != nil {
		return nil, err
	}

	return util.NewLocationRepositoryIterator(locs, mode), nil
}

// Location implements borges.Library interface.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	return l.location(id, false)
}

// AddLocation creates a new borges.Location if it does not exist.
func (l *Library) AddLocation(id borges.LocationID) (borges.Location, error) {
	_, err := l.Location(id)
	if err == nil {
		return nil, ErrLocationExists.New(id)
	}

	return l.location(id, true)
}

func (l *Library) location(id borges.LocationID, create bool) (borges.Location, error) {
	if loc, ok := l.locReg.Get(id); ok {
		return loc, nil
	}

	path := buildSivaPath(id, l.options.Bucket)
	loc, err := newLocation(id, l, path, create)
	if err != nil {
		return nil, err
	}

	l.locReg.Add(loc)

	return loc, nil
}

func buildSivaPath(id borges.LocationID, bucket int) string {
	siva := fmt.Sprintf("%s.siva", id)
	if bucket == 0 {
		return siva
	}

	r := []rune(id)
	var bucketDir string
	if len(r) < bucket {
		bucketDir = string(id) + strings.Repeat("-", bucket-len(r))
	} else {
		bucketDir = string(r[:bucket])
	}

	return filepath.Join(bucketDir, siva)
}

// Locations implements borges.Library interface.
func (l *Library) Locations() (borges.LocationIterator, error) {
	locs, err := l.locations()
	if err != nil {
		return nil, err
	}

	return util.NewLocationIterator(locs), nil
}

func (l *Library) locations() ([]borges.Location, error) {
	var locs []borges.Location

	pattern := filepath.Join(strings.Repeat("?", l.options.Bucket), "*.siva")
	sivas, err := butil.Glob(l.fs, pattern)
	if err != nil {
		return nil, err
	}

	for _, s := range sivas {
		siva := filepath.Base(s)
		loc, err := l.Location(toLocID(siva))
		if err != nil {
			continue
		}

		locs = append(locs, loc)
	}

	return locs, nil
}

// Library implements borges.Library interface.
func (l *Library) Library(id borges.LibraryID) (borges.Library, error) {
	if id == l.id {
		return l, nil
	}

	return nil, borges.ErrLibraryNotExists.New(id)
}

// Libraries implements borges.Library interface.
func (l *Library) Libraries() (borges.LibraryIterator, error) {
	libs := []borges.Library{l}
	return util.NewLibraryIterator(libs), nil
}

// Version returns version stored in metadata or -1 if not defined.
func (l *Library) Version() int {
	return l.metadata.Version()
}

// SetVersion sets the current version to the given number.
func (l *Library) SetVersion(n int) {
	if l.metadata == nil {
		l.metadata = NewLibraryMetadata(-1)
	}

	l.metadata.SetVersion(n)
}

// SaveMetadata writes the metadata to the library yaml file.
func (l *Library) SaveMetadata() error {
	if l.metadata != nil && l.metadata.dirty {
		return l.metadata.Save(l.fs)
	}

	return nil
}
