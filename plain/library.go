package plain

import (
	"context"
	"time"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
)

// LibraryOptions hold configuration options for the library.
type LibraryOptions struct {
	// Timeout set a timeout for library operations. Some operations could
	// potentially take long so timing out them will make an error be
	// returned. A 0 value sets a default value of 20 seconds.
	Timeout time.Duration
}

// Library represents a borges.Library implementation based on billy.Filesystems.
type Library struct {
	id   borges.LibraryID
	locs map[borges.LocationID]*Location
	libs map[borges.LibraryID]*Library
	opts *LibraryOptions
}

const (
	timeout = 20 * time.Second
)

// NewLibrary returns a new empty Library instance.
func NewLibrary(id borges.LibraryID, options *LibraryOptions) *Library {
	var opts *LibraryOptions
	if options == nil {
		opts = &LibraryOptions{}
	} else {
		opts = &(*options)
	}

	if opts.Timeout == 0 {
		opts.Timeout = timeout
	}

	return &Library{
		id:   id,
		locs: make(map[borges.LocationID]*Location, 0),
		libs: make(map[borges.LibraryID]*Library, 0),
		opts: opts,
	}
}

// ID returns the borges.LibraryID for this Library.
func (l *Library) ID() borges.LibraryID {
	return l.id
}

// AddLocation adds a Location to this Library.
func (l *Library) AddLocation(loc *Location) {
	loc.lib = l
	l.locs[loc.ID()] = loc
}

// AddLibrary adds a Library to this Library.
func (l *Library) AddLibrary(lib *Library) {
	l.libs[lib.ID()] = lib
}

// GetOrInit is not implemented. It honors the borges.Library interface.
func (l *Library) GetOrInit(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Init is not implemented. It honors the borges.Library interface.
func (l *Library) Init(borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

// Has returns true, the LibraryID and the LocationID if the given RepositoryID
// matches any repository at any location belonging to this Library.
func (l *Library) Has(id borges.RepositoryID) (bool, borges.LibraryID, borges.LocationID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	ok, loc, err := l.doHasOnLocations(ctx, id)
	if err != nil {
		return false, "", "", err
	}

	if ok {
		return ok, l.ID(), loc.ID(), err
	}

	ok, lib, loc, err := l.doHasOnLibraries(ctx, id)
	return ok, lib.ID(), loc.ID(), err
}

func (l *Library) doHasOnLocations(ctx context.Context, id borges.RepositoryID) (bool, *Location, error) {
	for _, loc := range l.locs {
		select {
		case <-ctx.Done():
			return false, nil, ctx.Err()
		default:
		}

		ok, err := loc.Has(id)
		if ok || err != nil {
			return ok, loc, err
		}
	}

	return false, nil, nil
}

func (l *Library) doHasOnLibraries(ctx context.Context, id borges.RepositoryID) (bool, *Library, *Location, error) {
	for _, lib := range l.libs {
		select {
		case <-ctx.Done():
			return false, nil, nil, ctx.Err()
		default:
		}

		ok, loc, err := lib.doHasOnLocations(ctx, id)
		if ok || err != nil {
			return ok, lib, loc, err
		}

		ok, lib, loc, err := lib.doHasOnLibraries(ctx, id)
		if ok || err != nil {
			return ok, lib, loc, err
		}
	}

	return false, nil, nil, nil
}

// Get open a repository with the given RepositoryID, it itereates all the
// library locations until this repository is found. If a repository with the
// given RepositoryID can't be found the ErrRepositoryNotExists is returned.
func (l *Library) Get(id borges.RepositoryID, m borges.Mode) (borges.Repository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	r, err := l.doGetOnLocations(ctx, id, m)
	if r != nil && err == nil {
		return r, nil
	}

	if err != nil && !borges.ErrRepositoryNotExists.Is(err) {
		return r, err
	}

	return l.doGetOnLibraries(ctx, id, m)
}

func (l *Library) doGetOnLocations(ctx context.Context, id borges.RepositoryID, m borges.Mode) (borges.Repository, error) {
	for _, loc := range l.locs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ok, err := loc.Has(id)
		if err != nil {
			return nil, err
		}

		if ok {
			return openRepository(loc, id, m)
		}
	}

	return nil, borges.ErrRepositoryNotExists.New(id)
}

func (l *Library) doGetOnLibraries(ctx context.Context, id borges.RepositoryID, m borges.Mode) (borges.Repository, error) {
	for _, lib := range l.libs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ok, loc, err := lib.doHasOnLocations(ctx, id)
		if ok && err == nil {
			return openRepository(loc, id, m)
		}

		if err != nil {
			return nil, err
		}

		ok, _, loc, err = lib.doHasOnLibraries(ctx, id)
		if ok && err == nil {
			return openRepository(loc, id, m)
		}

		if err != nil {
			return nil, err
		}
	}

	return nil, borges.ErrRepositoryNotExists.New(id)
}

// Repositories returns a RepositoryIterator that iterates through all the
// repositories contained in all Location contained in this Library.
func (l *Library) Repositories(
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	locs, err := mapLocationsToSlice(ctx, l.locs)
	if err != nil {
		return nil, err
	}

	return util.NewLocationRepositoryIterator(locs, mode), nil
}

func mapLocationsToSlice(
	ctx context.Context,
	m map[borges.LocationID]*Location,
) ([]borges.Location, error) {
	locs := make([]borges.Location, 0, len(m))
	for _, loc := range m {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		locs = append(locs, loc)
	}

	return locs, nil
}

// Location returns the a Location with the given ID, if exists, otherwise
// ErrLocationNotExists is returned.
func (l *Library) Location(id borges.LocationID) (borges.Location, error) {
	loc, ok := l.locs[id]
	if !ok {
		return nil, borges.ErrLocationNotExists.New(id)
	}

	return loc, nil
}

// Locations returns a LocationIterator that iterates through all locations
// contained in this Library.
func (l *Library) Locations() (borges.LocationIterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.Timeout)
	defer cancel()

	locs, err := mapLocationsToSlice(ctx, l.locs)
	if err != nil {
		return nil, err
	}

	return util.NewLocationIterator(locs), nil
}

// Library returns the Library with the given LibraryID, if a library can't
// be found ErrLibraryNotExists is returned.
func (l *Library) Library(id borges.LibraryID) (borges.Library, error) {
	lib, ok := l.libs[id]
	if !ok {
		return nil, borges.ErrLibraryNotExists.New(id)
	}

	return lib, nil
}

// Libraries returns a LibraryIterator that iterates through all libraries
// contained in this Library.
func (l *Library) Libraries() (borges.LibraryIterator, error) {
	libs := make([]borges.Library, len(l.libs))

	var i int
	for _, lib := range l.libs {
		libs[i] = lib
		i++
	}

	return util.NewLibraryIterator(libs), nil
}
