package libraries

import (
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"github.com/src-d/go-borges/util"
	"github.com/stretchr/testify/require"
)

func TestMergedIterators(t *testing.T) {
	var require = require.New(t)

	libs := setupSivaLibraries(t, siva.LibraryOptions{Bucket: 2})

	libIter, err := libs.Libraries()
	require.NoError(err)

	lib1Iter, err := libs.FilteredLibraries(filterLibID("lib1"))
	require.NoError(err)

	lib2Iter, err := libs.FilteredLibraries(filterLibID("lib2"))
	require.NoError(err)

	lib3Iter, err := libs.FilteredLibraries(filterLibID("lib3"))
	require.NoError(err)

	testLibIters(
		t,
		libIter,
		MergeLibraryIterators([]borges.LibraryIterator{
			lib2Iter,
			lib3Iter,
			lib1Iter,
		}),
	)

	libIter, err = libs.Libraries()
	require.NoError(err)

	var (
		locs         []borges.Location
		locsToMerge  []borges.LocationIterator
		reposToMerge []borges.RepositoryIterator
	)

	require.NoError(libIter.ForEach(func(l borges.Library) error {
		locIter, err := l.Locations()
		require.NoError(err)

		locsToMerge = append(locsToMerge, locIter)

		locIter, err = l.Locations()
		require.NoError(err)

		require.NoError(
			locIter.ForEach(func(loc borges.Location) error {
				locs = append(locs, loc)
				return nil
			}),
		)

		reposIter, err := l.Repositories(borges.ReadOnlyMode)
		require.NoError(err)

		reposToMerge = append(reposToMerge, reposIter)

		return nil
	}))

	testLocationIters(
		t,
		util.NewLocationIterator(locs),
		MergeLocationIterators(locsToMerge),
	)

	testRepositoryIters(
		t,
		util.NewLocationRepositoryIterator(locs, borges.ReadOnlyMode),
		MergeRepositoryIterators(reposToMerge),
	)
}

func filterLibID(id borges.LibraryID) FilterLibraryFunc {
	return func(l borges.Library) (bool, error) {
		return l.ID() == id, nil
	}
}

func testLibIters(t *testing.T, expected, iter borges.LibraryIterator) {
	var require = require.New(t)

	var expectedIDs []borges.LibraryID
	require.NoError(expected.ForEach(func(l borges.Library) error {
		expectedIDs = append(expectedIDs, l.ID())
		return nil
	}))

	var ids []borges.LibraryID
	require.NoError(iter.ForEach(func(l borges.Library) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expectedIDs, ids)
	expected.Close()
	iter.Close()
}

func testLocationIters(t *testing.T, expected, iter borges.LocationIterator) {
	var require = require.New(t)

	var expectedIDs []borges.LocationID
	require.NoError(expected.ForEach(func(l borges.Location) error {
		expectedIDs = append(expectedIDs, l.ID())
		return nil
	}))

	var ids []borges.LocationID
	require.NoError(iter.ForEach(func(l borges.Location) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expectedIDs, ids)
	expected.Close()
	iter.Close()
}

func testRepositoryIters(t *testing.T, expected, iter borges.RepositoryIterator) {
	var require = require.New(t)
	require.True(true)

	var expectedIDs []borges.RepositoryID
	require.NoError(expected.ForEach(func(r borges.Repository) error {
		expectedIDs = append(expectedIDs, r.ID())
		return nil
	}))

	var ids []borges.RepositoryID
	require.NoError(iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	}))

	require.ElementsMatch(expectedIDs, ids)
	expected.Close()
	iter.Close()
}
