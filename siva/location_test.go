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
	suite.Run(t, &locationSuite{transactional: false, bucket: 2})
	suite.Run(t, &locationSuite{transactional: true, bucket: 2})
}

type locationSuite struct {
	suite.Suite

	transactional bool
	bucket        int
	lib           *Library
}

func (s *locationSuite) SetupTest() {
	s.lib = setupLibrary(s.T(), "test", LibraryOptions{
		Transactional: s.transactional,
		Bucket:        s.bucket,
	})
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
	if s.transactional {
		require.NoError(err)
	} else {
		require.EqualError(err,
			borges.ErrNonTransactional.New().Error())
		err = r.Close()
		require.NoError(err)
	}

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
	if s.transactional {
		require.NoError(err)
	} else {
		require.EqualError(err,
			borges.ErrNonTransactional.New().Error())
		err = r.Close()
		require.NoError(err)
	}

	has, err = location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.True(has)

	has, err = location.Has("http://github.com/foo/no")
	require.NoError(err)
	require.True(has)
}

func (s *locationSuite) TestAddLocation() {
	require := s.Require()

	_, err := s.lib.AddLocation("foo-bar")
	require.True(ErrLocationExists.Is(err))

	const locationID = "new-location"
	const repoID = "new-repository"

	_, err = s.lib.Location(locationID)
	require.True(borges.ErrLocationNotExists.Is(err))

	l, err := s.lib.AddLocation(locationID)
	require.NoError(err)
	require.NotNil(l)

	r, err := l.Init(repoID)
	require.NoError(err)
	require.Equal(l.ID(), r.LocationID())

	_, err = r.R().CreateTag("test", plumbing.ZeroHash, nil)
	require.NoError(err)
	if s.transactional {
		require.NoError(r.Commit())
	} else {
		require.NoError(r.Close())
	}

	locs, err := s.lib.Locations()
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
	if s.transactional {
		require.True(ErrEmptyCommit.Is(r.Commit()))
	} else {
		require.NoError(r.Close())
	}

	r, err = s.lib.Get(repoID, borges.RWMode)
	require.NoError(err)
	if s.transactional {
		require.True(ErrEmptyCommit.Is(r.Commit()))
	} else {
		require.NoError(r.Close())
	}
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

	if s.transactional {
		require.NoError(repo.Commit())
	} else {
		require.NoError(repo.Close())
	}

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
	err = r.Commit()
	if s.transactional {
		require.NoError(err)
	} else {
		require.True(borges.ErrNonTransactional.Is(err))
		err = r.Close()
		require.NoError(err)
	}

	r, err = location.GetOrInit("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
	err = r.Commit()
	if s.transactional {
		require.True(ErrEmptyCommit.Is(err))
	} else {
		require.True(borges.ErrNonTransactional.Is(err))
		err = r.Close()
		require.NoError(err)
	}
}

func (s *locationSuite) TestFS() {
	require := s.Require()

	location, err := s.lib.Location("foo-bar")
	require.NoError(err)

	loc, ok := location.(*Location)
	require.True(ok)

	fs, err := loc.FS(borges.ReadOnlyMode)
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
		if s.transactional {
			require.NoError(err)
		} else {
			require.EqualError(err,
				borges.ErrNonTransactional.New().Error())
			err = e.Close()
			require.NoError(err)
		}
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
