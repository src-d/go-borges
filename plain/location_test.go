package plain

import (
	"fmt"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
)

func TestLocation(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New(), false)
	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	iter, err := location.Repositories()
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r *borges.Repository) error {
		ids = append(ids, r.ID)
		return nil
	})

	fmt.Println(ids)

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"github.com/foo/bar",
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

	r, err := location.Get("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
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

	iter, err := NewLocationIterator(NewLocation(fs, false))
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

	iter, err := NewLocationIterator(NewLocation(fs, true))
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

	iter, err := NewLocationIterator(NewLocation(fs, false))
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
