package legacysiva

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
	"gopkg.in/src-d/go-billy.v4"
	butil "gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"

	lru "github.com/hashicorp/golang-lru"
)

// LibraryOptions hold configuration options for the library.
type LibraryOptions struct {
	// RegistryCache is the maximum number of locations in the cache. A
	// value of 0 will be set a default value of 10000.
	RegistryCache int
	// Bucket level to use to search and create siva files.
	Bucket int
	// Cache specifies the shared cache used in repositories. If not defined
	// a new default cache will be created for each repository.
	Cache cache.Object
	// Timeout set a timeout for library operations. Some operations could
	// potentially take long so timing out them will make an error be
	// returned. A 0 value sets a default value of 20 seconds.
	Timeout time.Duration
}

// Library represents a borges.Library implementation based on siva files
// archiving rooted repositories using an old structure. See
// https://github.com/src-d/borges/blob/master/docs/using-borges/key-concepts.md#rooted-repository.
// It only supports read operations on the repositories and it doesn't support
// transactionality. Each siva file is managed as a single repository.
type Library struct {
	id    borges.LibraryID
	fs    billy.Filesystem
	cache *lru.Cache
	opts  *LibraryOptions
}

var _ borges.Library = (*Library)(nil)

const (
	registryCacheSize = 10000
	timeout           = 20 * time.Second
)

// NewLibrary builds a new Library.
func NewLibrary(
	id string,
	fs billy.Filesystem,
	options *LibraryOptions,
) (*Library, error) {
	var opts *LibraryOptions
	if options == nil {
		opts = &LibraryOptions{}
	} else {
		opts = &(*options)
	}

	if opts.RegistryCache <= 0 {
		opts.RegistryCache = registryCacheSize
	}

	cache, err := lru.New(opts.RegistryCache)
	if err != nil {
		return nil, err
	}

	if opts.Timeout == 0 {
		opts.Timeout = timeout
	}

	return &Library{
		id:    borges.LibraryID(id),
		fs:    fs,
		cache: cache,
		opts:  opts,
	}, nil
}

// ID implements the borges.Library interface.
func (l *Library) ID() borges.LibraryID {
	return l.id
}

// Init implements the borges.Library interface.
func (l *Library) Init(id borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Get implements the borges.Library interface. It only retrieves repositories
// in borges.ReadOnlyMode ignoring the given parameter.
func (l *Library) Get(
	id borges.RepositoryID,
	_ borges.Mode,
) (borges.Repository, error) {
	ok, _, locID, err := l.Has(id)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, borges.ErrRepositoryNotExists.New(id)
	}

	loc, err := l.Location(locID)
	if err != nil {
		return nil, err
	}

	return loc.Get(id, borges.ReadOnlyMode)
}

// GetOrInit implements the borges.Library interface.
func (l *Library) GetOrInit(_ borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has implements the borges.Library interface.
func (l *Library) Has(
	id borges.RepositoryID,
) (bool, borges.LibraryID, borges.LocationID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
	if err != nil {
		return false, "", "", err
	}

	it := util.NewLocationIterator(locs)
	defer it.Close()

	for {
		loc, err := it.Next()
		if err == io.EOF {
			return false, "", "", nil
		}

		if err != nil {
			return false, "", "", err
		}

		has, err := loc.Has(id)
		if err != nil {
			return false, "", "", err
		}

		if has {
			return true, l.id, loc.ID(), nil
		}
	}
}

// Repositories implements the borges.Library interface. It only retrieves
// repositories in borges.ReadOnlyMode ignoring the given parameter.
func (l *Library) Repositories(
	_ borges.Mode,
) (borges.RepositoryIterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
	if err != nil {
		return nil, err
	}

	return util.NewLocationRepositoryIterator(
		locs,
		borges.ReadOnlyMode,
	), nil
}

// Location implements the borges.Library interface.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	return l.location(id)
}

func (l *Library) location(id borges.LocationID) (borges.Location, error) {
	if loc, ok := l.cache.Get(id); ok {
		return loc.(*Location), nil
	}

	path := buildSivaPath(id, l.opts.Bucket)
	loc, err := newLocation(id, l, path)
	if err != nil {
		return nil, err
	}

	l.cache.Add(loc.ID(), loc)
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

// Locations implements the borges.Library interface.
func (l *Library) Locations() (borges.LocationIterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	locs, err := l.locations(ctx)
	if err != nil {
		return nil, err
	}

	return util.NewLocationIterator(locs), nil
}

func (l *Library) locations(ctx context.Context) ([]borges.Location, error) {
	var locs []borges.Location

	pattern := filepath.Join(strings.Repeat("?", l.opts.Bucket), "*.siva")
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

func toLocID(file string) borges.LocationID {
	id := strings.TrimSuffix(file, ".siva")
	return borges.LocationID(id)
}
