package siva

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	id       borges.LibraryID
	fs       billy.Filesystem
	tmp      billy.Filesystem
	locReg   *locationRegistry
	locMu    sync.Mutex
	options  *LibraryOptions
	metadata *libMetadata
}

// LibraryOptions hold configuration options for the library.
type LibraryOptions struct {
	// Transactional enables transactions for repository writes.
	Transactional bool
	// TransactionTimeout is the time it will wait while another transaction
	// is being done before error. 0 means default.
	TransactionTimeout time.Duration
	// Timeout set a timeout for library operations. Some operations could
	// potentially take long so timing out them will make an error be
	// returned. A 0 value sets a default value of 20 seconds.
	Timeout time.Duration
	// RegistryCache is the maximum number of locations in the cache. A value
	// of 0 will be set a default value of 10000.
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
	// MetadataReadOnly doesn't create or modify metadata for the library.
	MetadataReadOnly bool
}

var _ borges.Library = (*Library)(nil)

const (
	timeout           = 20 * time.Second
	txTimeout         = 60 * time.Second
	registryCacheSize = 10000
)

// NewLibrary creates a new siva.Library. When is not in MetadataReadOnly it
// will generate an id if not provided the first time the metadata is created.
func NewLibrary(
	id string,
	fs billy.Filesystem,
	options *LibraryOptions,
) (*Library, error) {
	var ops *LibraryOptions
	if options == nil {
		ops = &LibraryOptions{}
	} else {
		ops = &(*options)
	}

	var (
		metadata *libMetadata
		err      error
	)

	if ops.MetadataReadOnly {
		metadata, err = loadLibraryMetadata(fs)
	} else {
		metadata, err = loadOrCreateLibraryMetadata(id, fs)
	}

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if metadata != nil && id == "" {
		id = metadata.ID
	}

	if ops.RegistryCache <= 0 {
		ops.RegistryCache = registryCacheSize
	}

	lr, err := newLocationRegistry(ops.RegistryCache)
	if err != nil {
		return nil, err
	}

	if ops.TransactionTimeout == 0 {
		ops.TransactionTimeout = txTimeout
	}

	if ops.Timeout == 0 {
		ops.Timeout = timeout
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
	ctx, cancel := context.WithTimeout(context.Background(), l.options.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
	if err != nil {
		return false, "", "", err
	}

	it := util.NewLocationIterator(locs)
	defer it.Close()

	for {
		location, err := it.Next()
		if err == io.EOF {
			return false, "", "", nil
		}

		if err != nil {
			return false, "", "", err
		}

		loc, _ := location.(*Location)

		has, err := loc.has(ctx, name)
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
	ctx, cancel := context.WithTimeout(context.Background(), l.options.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
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
	l.locMu.Lock()
	defer l.locMu.Unlock()

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
	ctx, cancel := context.WithTimeout(context.Background(), l.options.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
	if err != nil {
		return nil, err
	}

	return util.NewLocationIterator(locs), nil
}

func (l *Library) locations(ctx context.Context) ([]borges.Location, error) {
	var locs []borges.Location

	pattern := filepath.Join(
		strings.Repeat("?", l.options.Bucket),
		"*.siva",
	)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sivas, err := butil.Glob(l.fs, pattern)
	if err != nil {
		return nil, err
	}

	for _, s := range sivas {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		siva := filepath.Base(s)
		loc, err := l.Location(toLocID(siva))
		if err != nil {
			continue
		}

		locs = append(locs, loc)
	}

	return locs, nil
}

// Version returns version stored in metadata or -1 if not defined.
func (l *Library) Version() (int, error) {
	if l.metadata != nil {
		return l.metadata.version()
	}

	return -1, nil
}

// SetVersion sets the current version to the given number.
func (l *Library) SetVersion(n int) error {
	if l.metadata == nil {
		return nil
	}

	l.metadata.setVersion(n)
	return l.metadata.save()
}
