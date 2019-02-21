package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestAddLocation(t *testing.T) {
	require := require.New(t)
	fs := setupFS(t)

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

func TestLocationHasURL(t *testing.T) {
	require := require.New(t)

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

	lib, err := NewLibrary("test", memfs.New(), LibraryOptions{})
	require.NoError(err)

	loc, err := lib.AddLocation("location")
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

	found, _, _, err := lib.Has("github.com/src-d/invalid")
	require.NoError(err)
	require.False(found)

	for _, id := range repoIDs {
		found, _, l, err := lib.Has(borges.RepositoryID(id))
		require.NoError(err)
		require.True(found)
		require.Equal("location", string(l))
	}
}

func TestLocation(t *testing.T) {
	require := require.New(t)

	id, err := borges.NewRepositoryID("http://github.com/foo/bar")
	require.NoError(err)

	lib, err := NewLibrary("test", memfs.New(), LibraryOptions{})
	require.NoError(err)

	var location borges.Location
	location, err = lib.AddLocation("foo")
	require.NoError(err)

	r, err := location.Init(id)
	require.NoError(err)
	require.NotNil(r)

	iter, err := location.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"github.com/foo/bar",
	})
}
