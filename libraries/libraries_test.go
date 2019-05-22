package libraries

import (
	"io"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"

	"github.com/stretchr/testify/suite"
)

func TestLibraries(t *testing.T) {
	suite.Run(t, &librariesSuite{bucket: 2, transactional: false})
	suite.Run(t, &librariesSuite{bucket: 2, transactional: true})
}

type librariesSuite struct {
	suite.Suite

	bucket        int
	transactional bool
	libs          *Libraries
}

func (s *librariesSuite) SetupSuite() {
	s.libs = setupSivaLibraries(s.T(), siva.LibraryOptions{
		Bucket:        s.bucket,
		Transactional: s.transactional,
	})
}

func (s *librariesSuite) TestNotImplemented() {
	var require = s.Require()

	_, err := s.libs.Init("foo")
	require.True(borges.ErrNotImplemented.Is(err))

	_, err = s.libs.GetOrInit("foo")
	require.True(borges.ErrNotImplemented.Is(err))
}

func (s *librariesSuite) TestLibraryAndLocationAndHasAndGet() {
	var require = s.Require()

	for lib, locations := range testLibs {
		_, err := s.libs.Library(lib)
		require.NoError(err)
		for loc, repos := range locations {
			_, err := s.libs.Location(loc)
			require.NoError(err)
			for _, repo := range repos {
				ok, libID, locID, err := s.libs.Has(repo)
				require.NoError(err)
				require.True(ok, repo.String())
				require.Equal(lib, libID)
				require.Equal(loc, locID)

				_, err = s.libs.Get(repo, borges.ReadOnlyMode)
				require.NoError(err)
			}
		}
	}
}

func (s *librariesSuite) TestRepositories() {
	var require = s.Require()

	var expected []borges.RepositoryID
	for _, locations := range testLibs {
		for _, repos := range locations {
			for _, repo := range repos {
				expected = append(expected, repo)
			}
		}
	}

	iter, err := s.libs.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	require.NoError(iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestLocations() {
	var require = s.Require()

	var expected []borges.LocationID
	for _, locations := range testLibs {
		for loc := range locations {
			expected = append(expected, loc)
		}
	}

	iter, err := s.libs.Locations()
	require.NoError(err)

	var ids []borges.LocationID
	require.NoError(iter.ForEach(func(l borges.Location) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestLibraries() {
	var require = s.Require()

	var expected []borges.LibraryID
	for lib := range testLibs {
		expected = append(expected, lib)
	}

	iter, err := s.libs.Libraries()
	require.NoError(err)

	var ids []borges.LibraryID
	require.NoError(iter.ForEach(func(l borges.Library) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestFilteredLibraries() {
	var require = s.Require()

	var filter FilterLibraryFunc = func(lib borges.Library) (bool, error) {
		_, ok := lib.(*plain.Library)
		return ok, nil
	}

	iter, err := s.libs.FilteredLibraries(filter)
	require.NoError(err)

	_, err = iter.Next()
	require.EqualError(err, io.EOF.Error())

	filter = func(lib borges.Library) (bool, error) {
		ok, _, _, err := lib.Has(borges.RepositoryID("github.com/rtyley/small-test-repo"))
		return ok, err
	}

	iter, err = s.libs.FilteredLibraries(filter)
	require.NoError(err)

	lib, err := iter.Next()
	require.NoError(err)
	require.Equal(borges.LibraryID("lib2"), lib.ID())

	_, err = iter.Next()
	require.EqualError(err, io.EOF.Error())
}
