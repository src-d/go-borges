package test

import (
	borges "github.com/src-d/go-borges"

	"github.com/stretchr/testify/suite"
)

// There are two suites that can be used for libraries that support one or
// multiple levels of libraries. The tests have to define the functions
// LibrarySingle and LibraryNested to create the libraries used for testing.
//
// The hierarchy that these functions has to create is as follows:
//
// LibrarySingle
//	Library "foo"
//		Location "foo-qux"
//			Repository "github.com/foo/qux"
//		Location "foo-bar"
//			Repository "github.com/foo/bar"
//
// LibraryNested
//	Library "baz"
//		Library "foo"
//			Location "foo-qux"
//				Repository "github.com/foo/qux"
//			Location "foo-bar"
//				Repository "github.com/foo/bar"
//		Library "nested"
//			Library "deep"
//				Location "deep-qux"
//					Repository "github.com/deep/qux"
//				Location "deep-bar"
//					Repository "github.com/deep/bar"

// LibrarySuite defines tests for single level libraries
type LibrarySuite struct {
	suite.Suite
	LibrarySingle func() borges.Library
}

// LibraryNestedSuite defines tests for multiple level libraries
type LibraryNestedSuite struct {
	LibrarySuite
	LibraryNested func() borges.Library
}

func (s *LibrarySuite) TestLibrary() {
	require := s.Require()

	var library borges.Library
	library = s.LibrarySingle()
	require.NotNil(library)
	require.Equal(library.ID(), borges.LibraryID("foo"))
}

func (s *LibrarySuite) TestHas() {
	require := s.Require()
	l := s.LibrarySingle()

	ok, lib, loc, err := l.Has("github.com/foo/qux")
	require.NoError(err)
	require.True(ok)
	require.Equal(borges.LibraryID("foo"), lib)
	require.Equal(borges.LocationID("foo-qux"), loc)
}

func (s *LibraryNestedSuite) TestHasNestedLibrary() {
	require := s.Require()

	library := s.LibraryNested()

	ok, lib, loc, err := library.Has("github.com/foo/qux")
	require.NoError(err)
	require.True(ok)
	require.Equal(borges.LibraryID("foo"), lib)
	require.Equal(borges.LocationID("foo-qux"), loc)
}

func (s *LibrarySuite) TestGet() {
	require := s.Require()
	library := s.LibrarySingle()

	r, err := library.Get("github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("foo-qux"), r.LocationID())
}

func (s *LibraryNestedSuite) TestGetNestedLibrary() {
	require := s.Require()
	library := s.LibraryNested()

	r, err := library.Get("github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("foo-qux"), r.LocationID())
}

func (s *LibraryNestedSuite) TestGetDeepNestedLibrary() {
	require := s.Require()
	l := s.LibraryNested()

	r, err := l.Get("github.com/deep/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("deep-qux"), r.LocationID())
}

func (s *LibrarySuite) TestGetNotFound() {
	require := s.Require()
	l := s.LibrarySingle()

	r, err := l.Get("github.com/foo/nope", borges.RWMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))
	require.Nil(r)
}

func (s *LibrarySuite) TestLocations() {
	require := s.Require()
	l := s.LibrarySingle()

	iter, err := l.Locations()
	require.NoError(err)

	var ids []borges.LocationID
	err = iter.ForEach(func(r borges.Location) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.LocationID{
		"foo-bar",
		"foo-qux",
	})
}

func (s *LibrarySuite) TestLocation() {
	require := s.Require()
	l := s.LibrarySingle()

	r, err := l.Location("foo-bar")
	require.NoError(err)
	require.NotNil(r)
}

func (s *LibrarySuite) TestLocationNotFound() {
	require := s.Require()
	l := s.LibrarySingle()

	r, err := l.Location("foo")
	require.True(borges.ErrLocationNotExists.Is(err))
	require.Nil(r)
}

func (s *LibraryNestedSuite) TestLibrary() {
	require := s.Require()
	l := s.LibraryNested()

	r, err := l.Library("foo")
	require.NoError(err)
	require.NotNil(r)

	r, err = l.Library("nested")
	require.NoError(err)
	require.NotNil(r)
}

func (s *LibraryNestedSuite) TestLibraries() {
	require := s.Require()
	l := s.LibraryNested()

	iter, err := l.Libraries()
	require.NoError(err)

	var ids []borges.LibraryID
	err = iter.ForEach(func(r borges.Library) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.LibraryID{
		"foo",
		"nested",
	})
}

func (s *LibrarySuite) TestLibraryNotFound() {
	require := s.Require()
	l := s.LibrarySingle()

	r, err := l.Library("bar")
	require.True(borges.ErrLibraryNotExists.Is(err))
	require.Nil(r)
}

func (s *LibrarySuite) TestRepositories() {
	require := s.Require()
	l := s.LibrarySingle()

	iter, err := l.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"github.com/foo/qux",
		"github.com/foo/bar",
	})
}
