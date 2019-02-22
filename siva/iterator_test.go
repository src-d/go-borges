package siva

import (
	"io"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/config"
)

func TestRepositoryIterator(t *testing.T) {
	var require = require.New(t)

	lib := setupLibrary(t, "test", LibraryOptions{})
	loc, err := lib.Location("foo-bar")
	require.NoError(err)

	iter, err := loc.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	i, ok := iter.(*repositoryIterator)
	require.True(ok)
	require.NotNil(i)

	require.Equal(loc, i.loc)
	require.Equal(borges.ReadOnlyMode, i.mode)
	require.Equal(0, i.pos)
	require.Len(i.remotes, 1)

	remote := i.remotes[0]
	name := "https://github.com/foo/bar"
	require.Equal(name, remote.Name)
	require.Equal([]string{name}, remote.URLs)
	require.Equal(
		[]config.RefSpec{"+refs/heads/*:refs/remotes/0168e2c7-eedc-7358-0a09-39ba833bdd54/*"},
		remote.Fetch,
	)

	r, err := i.Next()
	require.NoError(err)
	require.NotNil(r)
	require.Equal(toRepoID(name), r.ID())
	require.Equal(loc.ID(), r.LocationID())
	require.Equal(borges.ReadOnlyMode, r.Mode())

	_, err = i.Next()
	require.EqualError(err, io.EOF.Error())

	iter, err = loc.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var count int
	err = iter.ForEach(func(_ borges.Repository) error {
		count++
		return nil
	})
	require.NoError(err)
	require.Equal(len(i.remotes), count)
}
