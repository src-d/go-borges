package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestLocation(t *testing.T) {
	suite.Run(t, &locationSuite{transactional: false})
	suite.Run(t, &locationSuite{transactional: true})
}

type locationSuite struct {
	suite.Suite

	transactional bool
	lib           *Library
}

func (s *locationSuite) SetupTest() {
	require := s.Require()

	fs := setupFS(s.T())
	lib, err := NewLibrary("test", fs, LibraryOptions{
		Transactional: s.transactional,
	})
	require.NoError(err)

	s.lib = lib
}

func (s *locationSuite) TestCreate() {
	require := s.Require()

	id, err := borges.NewRepositoryID("http://github.com/foo/bar")
	require.NoError(err)

	var location borges.Location
	location, err = s.lib.AddLocation("foo")
	require.NoError(err)

	r, err := location.Init(id)
	require.NoError(err)
	require.NotNil(r)
	err = r.Commit()
	require.NoError(err)

	iter, err := location.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return r.Close()
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"github.com/foo/bar",
	})
}

func (s *locationSuite) TestHas() {
	require := require.New(s.T())

	location, err := s.lib.Location("foo-bar")
	require.NoError(err)

	// has, err := location.Has("http://github.com/foo/bar")
	has, err := location.Has("github.com/foo/bar")
	require.NoError(err)
	require.True(has)

	has, err = location.Has("http://github.com/foo/no")
	require.NoError(err)
	require.False(has)
}

func (s *locationSuite) TestInitExists() {
	require := require.New(s.T())

	location, err := s.lib.Location("foo-bar")
	require.NoError(err)

	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.True(has)

	has, err = location.Has("http://github.com/foo/no")
	require.NoError(err)
	require.False(has)

	r, err := location.Init("http://github.com/foo/no")
	require.NoError(err)
	err = r.Commit()
	require.NoError(err)

	has, err = location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.True(has)

	has, err = location.Has("http://github.com/foo/no")
	require.NoError(err)
	require.True(has)
}

func (s *locationSuite) TestAddLocation() {
	require := s.Require()
	fs := setupFS(s.T())

	lib, err := NewLibrary("test", fs, LibraryOptions{
		Transactional: true,
	})
	require.NoError(err)

	_, err = lib.AddLocation("foo-bar")
	require.True(ErrLocationExists.Is(err))

	const locationID = "new-location"
	const repoID = "new-repository"

	_, err = lib.Location(locationID)
	require.True(borges.ErrLocationNotExists.Is(err))

	l, err := lib.AddLocation(locationID)
	require.NoError(err)
	require.NotNil(l)

	r, err := l.Init(repoID)
	require.NoError(err)
	require.Equal(l.ID(), r.LocationID())
	_, err = r.R().CreateTag("test", plumbing.ZeroHash, nil)
	require.NoError(err)
	err = r.Commit()
	require.NoError(err)

	locs, err := lib.Locations()
	require.NoError(err)

	found := false
	locs.ForEach(func(l borges.Location) error {
		if l.ID() == locationID {
			found = true
		}
		return nil
	})
	require.True(found, "created location not found")

	r, err = l.Get(repoID, borges.RWMode)
	require.NoError(err)
	err = r.Commit()
	require.NoError(err)

	r, err = lib.Get(repoID, borges.RWMode)
	require.NoError(err)
	err = r.Commit()
	require.NoError(err)
}

func (s *locationSuite) TestHasURL() {
	require := s.Require()

	repoName := borges.RepositoryID("0168e2c7-eedc-7358-0a09-39ba833bdd54")
	repoURLs := []string{
		"https://github.com/src-d/https",
		"http://github.com/src-d/http",
		"git://github.com/src-d/git",
		"file://github.com/src-d/file",
		"git@github.com:src-d/ssh",
	}
	repoIDs := []string{
		"github.com/src-d/https",
		"github.com/src-d/http",
		"github.com/src-d/git",
		"github.com/src-d/file",
		"github.com/src-d/ssh",
	}

	loc, err := s.lib.AddLocation("location")
	require.NoError(err)

	repo, err := loc.Init(repoName)
	require.NoError(err)
	r := repo.R()

	config, err := r.Config()
	require.NoError(err)

	remote, ok := config.Remotes[repoName.String()]
	require.True(ok)

	remote.URLs = repoURLs
	err = r.Storer.SetConfig(config)
	require.NoError(err)

	err = repo.Commit()
	require.NoError(err)

	found, _, _, err := s.lib.Has("github.com/src-d/invalid")
	require.NoError(err)
	require.False(found)

	for _, id := range repoIDs {
		found, _, l, err := s.lib.Has(borges.RepositoryID(id))
		require.NoError(err)
		require.True(found)
		require.Equal("location", string(l))
	}
}

func (s *locationSuite) TestGetOrInit() {
	require := s.Require()

	location, err := s.lib.AddLocation("test")
	require.NoError(err)

	_, err = location.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))

	r, err := location.GetOrInit("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
	r.Commit()

	r, err = location.GetOrInit("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
	r.Commit()
}

func (s *locationSuite) TestFS() {
	require := s.Require()

	location, err := s.lib.Location("foo-bar")
	require.NoError(err)

	loc, ok := location.(*Location)
	require.True(ok)

	fs, err := loc.FS()
	require.NoError(err)

	stat, err := fs.Stat("objects/pack/pack-bb25e08fc37bda477660be0609a356f6d1e65ffc.pack")
	require.NoError(err)
	require.Equal(int64(207), stat.Size())
}

func (s *locationSuite) TestRepositories() {
	require := s.Require()

	repoIDs := []string{
		"github.com/src-d/https",
		"github.com/src-d/http",
		"github.com/src-d/git",
		"github.com/src-d/file",
		"github.com/src-d/ssh",
	}

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	for _, id := range repoIDs {
		e, err := loc.Init(borges.RepositoryID(id))
		require.NoError(err)
		err = e.Commit()
		require.NoError(err)
	}

	it, err := loc.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var names []string
	err = it.ForEach(func(r borges.Repository) error {
		names = append(names, r.ID().String())
		return r.Close()
	})
	require.NoError(err)

	require.ElementsMatch(repoIDs, names)
}
