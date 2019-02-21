package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
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
