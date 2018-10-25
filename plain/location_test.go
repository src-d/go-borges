package plain

import (
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestLocation(t *testing.T) {
	require := require.New(t)

	id, err := borges.NewRepositoryID("http://github.com/foo/bar")
	require.NoError(err)

	var location borges.Location
	location = NewLocation(memfs.New(), false)
	r, err := location.Init(id)
	require.NoError(err)
	require.NotNil(r)

	iter, err := location.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r *borges.Repository) error {
		ids = append(ids, r.ID)
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"github.com/foo/bar.git",
	})
}

func TestLocation_Has(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)
	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.False(has)
}

func TestLocation_Init(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	remote, err := r.Remote("origin")
	require.NoError(err)
	require.NotNil(remote)

	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.True(has)
}

func TestLocation_InitExists(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	r, err = location.Init("http://github.com/foo/bar")
	require.True(ErrRepositoryExists.Is(err))
	require.Nil(r)
}

func TestLocation_Get(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)

	_, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/bar", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)
}

func TestLocation_Get_ReadOnlyMode(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)

	_, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)
	require.NotNil(r)

	err = r.Storer.SetReference(plumbing.NewHashReference("foo", plumbing.ZeroHash))
	require.True(util.ErrReadOnlyStorer.Is(err))
}

func TestLocation_GetOrInit(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), true)

	r, err := location.GetOrInit("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	r, err = location.GetOrInit("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
}

func TestLocationIterator_Next(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo/.git")
	fs.MkdirAll("qux", 0755)

	iter, err := NewLocationIterator(NewLocation(fs, false), borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r *borges.Repository) error {
		ids = append(ids, r.ID)
		return nil
	})

	require.NoError(err)
	require.Len(ids, 1)
	require.Equal(ids[0].String(), "foo")
}

func TestLocationIterator_NextBare(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo")
	fs.MkdirAll("qux", 0755)

	iter, err := NewLocationIterator(NewLocation(fs, true), borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r *borges.Repository) error {
		ids = append(ids, r.ID)
		return nil
	})

	require.NoError(err)
	require.Len(ids, 1)
	require.Equal(ids[0].String(), "foo")
}

func TestLocationIterator_NextDeep(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo/qux/.git")
	createValidDotGit(require, fs, "foo/bar/.git")
	fs.MkdirAll("qux", 0755)
	createValidDotGit(require, fs, "qux/bar/baz/.git")
	createValidDotGit(require, fs, "qux/bar/.git")

	iter, err := NewLocationIterator(NewLocation(fs, false), borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r *borges.Repository) error {
		ids = append(ids, r.ID)
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"foo/qux", "foo/bar", "qux/bar",
	})
}
