package libraries

import (
	"io"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	"github.com/src-d/go-borges/util"
)

// MergeRepositoryIterators builds a new iterator from the given ones.
func MergeRepositoryIterators(iters []borges.RepositoryIterator) borges.RepositoryIterator {
	return &mergedRepoIter{iters: iters}
}

type mergedRepoIter struct {
	iters []borges.RepositoryIterator
}

var _ borges.RepositoryIterator = (*mergedRepoIter)(nil)

// Next implements the borges.RepositoryIterator interface.
func (i *mergedRepoIter) Next() (borges.Repository, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	repo, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return repo, nil
}

// ForEach implements the borges.RepositoryIterator interface.
func (i *mergedRepoIter) ForEach(cb func(borges.Repository) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachRepositoryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.RepositoryIterator interface.
func (i *mergedRepoIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// MergeLocationIterators builds a new iterator from the given ones.
func MergeLocationIterators(iters []borges.LocationIterator) borges.LocationIterator {
	return &mergedLocationIter{iters: iters}
}

type mergedLocationIter struct {
	iters []borges.LocationIterator
}

var _ borges.LocationIterator = (*mergedLocationIter)(nil)

// Next implements the borges.LocationIterator interface.
func (i *mergedLocationIter) Next() (borges.Location, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	loc, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return loc, nil
}

// ForEach implements the borges.LocationIterator interface.
func (i *mergedLocationIter) ForEach(cb func(borges.Location) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLocatorIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.LocationIterator interface.
func (i *mergedLocationIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// MergeLibraryIterators builds a new iterator from the given ones.
func MergeLibraryIterators(iters []borges.LibraryIterator) borges.LibraryIterator {
	return &mergedLibIter{iters: iters}
}

type mergedLibIter struct {
	iters []borges.LibraryIterator
}

var _ borges.LibraryIterator = (*mergedLibIter)(nil)

// Next implements the borges.LibraryIterator interface.
func (i *mergedLibIter) Next() (borges.Library, error) {
	if len(i.iters) == 0 {
		return nil, io.EOF
	}

	lib, err := i.iters[0].Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		i.iters = i.iters[1:]
		return i.Next()
	}

	return lib, nil
}

// ForEach implements the borges.LibraryIterator interface.
func (i *mergedLibIter) ForEach(cb func(borges.Library) error) error {
	for _, iter := range i.iters {
		if err := util.ForEachLibraryIterator(iter, cb); err != nil {
			return err
		}
	}

	return nil
}

// Close implements the borges.LibraryIterator interface.
func (i *mergedLibIter) Close() {
	for _, iter := range i.iters {
		iter.Close()
	}
}

// RepositoryDefaultIter returns a borges.RepositoryIterator with no specific
// iteration order.
func RepositoryDefaultIter(
	l *Libraries,
	mode borges.Mode) (borges.RepositoryIterator, error) {

	var repositories []borges.RepositoryIterator
	for _, lib := range l.libs {
		repos, err := lib.Repositories(mode)
		if err != nil {
			return nil, err
		}

		repositories = append(repositories, repos)
	}

	return MergeRepositoryIterators(repositories), nil
}

// RepoIterJumpPlainLibraries returns a borges.RepositoryIterator with the same
// properties as the one returned by RepoIterJumpLibraries but using only those
// libraries of type plain.Library.
func RepoIterJumpPlainLibraries(
	libs *Libraries,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	var filter FilterLibraryFunc = func(lib borges.Library) (bool, error) {
		_, ok := lib.(*plain.Library)
		return ok, nil
	}

	libIter, err := libs.FilteredLibraries(filter)
	if err != nil {
		return nil, err
	}

	return repoIterJumpLibraries(libIter, mode)
}

// RepoIterJumpLibraries returns a borges.RepositoryIterator whose order will
// be a returned borges.Repository from a different library each time,
// that is: repo from lib1, repo from lib2, repo from lib3, repo from lib1 ...
func RepoIterJumpLibraries(
	libs *Libraries,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	libIter, err := libs.Libraries()
	if err != nil {
		return nil, err
	}

	return repoIterJumpLibraries(libIter, mode)
}

func repoIterJumpLibraries(
	libIter borges.LibraryIterator,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	var repoIters []borges.RepositoryIterator
	err := libIter.ForEach(func(l borges.Library) error {
		ri, err := l.Repositories(mode)
		if err == nil {
			repoIters = append(repoIters, ri)
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return newJumpLibsRepoIter(repoIters), nil
}

type jumpLibsRepoIter struct {
	repoIters []borges.RepositoryIterator
	idx       int
	closed    []bool
}

var _ borges.RepositoryIterator = (*jumpLibsRepoIter)(nil)

func newJumpLibsRepoIter(repoIters []borges.RepositoryIterator) *jumpLibsRepoIter {
	return &jumpLibsRepoIter{
		repoIters: repoIters,
		closed:    make([]bool, len(repoIters)),
	}
}

// Next implements the borges.RepositoryIterator interface.
func (i *jumpLibsRepoIter) Next() (borges.Repository, error) {
	if len(i.repoIters) == 0 || i.isClosed() {
		return nil, io.EOF
	}

	if i.idx >= len(i.repoIters) {
		i.idx = 0
	}

	repo, err := i.repoIters[i.idx].Next()
	if err != nil {
		if err == io.EOF {
			i.repoIters[i.idx].Close()
			i.closed[i.idx] = true
			i.idx++
			return i.Next()
		}

		return nil, err
	}

	i.idx++
	return repo, nil
}

func (i *jumpLibsRepoIter) isClosed() bool {
	for _, closed := range i.closed {
		if !closed {
			return false
		}
	}

	return true
}

// ForEach implements the borges.RepositoryIterator interface.
func (i *jumpLibsRepoIter) ForEach(cb func(borges.Repository) error) error {
	return util.ForEachRepositoryIterator(i, cb)
}

// Close implements the borges.RepositoryIterator interface.
func (i *jumpLibsRepoIter) Close() {
	for _, ri := range i.repoIters {
		ri.Close()
	}
}

// RepoIterSivasJumpLocations returns a borges.RepositoryIterator with the same
// properties as the one returned by RepoIterJumpLocations but using only those
// libraries of type siva.Library.
func RepoIterSivasJumpLocations(
	libs *Libraries,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	var filter FilterLibraryFunc = func(lib borges.Library) (bool, error) {
		_, ok := lib.(*siva.Library)
		return ok, nil
	}

	libIter, err := libs.FilteredLibraries(filter)
	if err != nil {
		return nil, err
	}

	return repoIterJumpLocations(libIter, mode)
}

// RepoIterJumpLocations returns a borges.RepositoryIterator whose order will
// be all the repositories from a location from a different library each time,
// that is: repos from loc1/lib1, repos from loc1/lib2, repos from loc2/lib1, ...
func RepoIterJumpLocations(
	libs *Libraries,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	libIter, err := libs.Libraries()
	if err != nil {
		return nil, err
	}

	return repoIterJumpLocations(libIter, mode)
}

func repoIterJumpLocations(
	libIter borges.LibraryIterator,
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	var locsIter []borges.LocationIterator
	err := libIter.ForEach(func(lib borges.Library) error {
		locIter, err := lib.Locations()
		if err == nil {
			locsIter = append(locsIter, locIter)
		}

		return err
	})

	if err != nil {
		return nil, err
	}

	return newJumpLocsRepoIter(locsIter, mode), nil
}

type jumpLocsRepoIter struct {
	locIters []borges.LocationIterator
	idx      int
	mode     borges.Mode
	repoIter borges.RepositoryIterator
	closed   bool
}

var _ borges.RepositoryIterator = (*jumpLocsRepoIter)(nil)

func newJumpLocsRepoIter(
	locIters []borges.LocationIterator,
	mode borges.Mode,
) *jumpLocsRepoIter {
	return &jumpLocsRepoIter{
		locIters: locIters,
		mode:     mode,
	}
}

// Next implements the borges.RepositoryIterator interface.
func (i *jumpLocsRepoIter) Next() (borges.Repository, error) {
	if len(i.locIters) == 0 || i.closed {
		return nil, io.EOF
	}

	if i.repoIter == nil {
		if err := i.nextRepoIter(); err != nil {
			return nil, err
		}
	}

	repo, err := i.repoIter.Next()
	if err != nil {
		if err == io.EOF {
			if err := i.nextRepoIter(); err != nil {
				return nil, err
			}

			return i.Next()
		}

		return nil, err
	}

	return repo, nil
}

func (i *jumpLocsRepoIter) nextRepoIter() error {
	n := i.idx
	var stop bool
	for {
		if n >= len(i.locIters) {
			n = 0
		}

		loc, err := i.locIters[n].Next()
		if err != nil {
			if err == io.EOF {
				i.locIters[n].Close()
				stop = true
				n++
				if n == i.idx && stop {
					i.closed = true
					return err
				}

				continue
			}

			return err
		}

		repoIter, err := loc.Repositories(i.mode)
		if err != nil {
			return err
		}

		i.repoIter = repoIter
		i.idx = n + 1
		break
	}

	return nil
}

// ForEach implements the borges.RepositoryIterator interface.
func (i *jumpLocsRepoIter) ForEach(cb func(borges.Repository) error) error {
	return util.ForEachRepositoryIterator(i, cb)
}

// Close implements the borges.RepositoryIterator interface.
func (i *jumpLocsRepoIter) Close() {
	i.repoIter.Close()
	for _, li := range i.locIters {
		li.Close()
	}

	i.closed = true
}
