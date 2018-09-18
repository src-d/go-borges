package plain

import (
	"fmt"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
)

func TestLocation_Has(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New())
	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.False(has)
}

func TestLocation_Init(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New())

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

	location := NewLocation(memfs.New())

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	r, err = location.Init("http://github.com/foo/bar")
	require.True(ErrRepositoryExists.Is(err))
	require.Nil(r)
}

func TestLocation_Get(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New())

	_, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
}

func TestLocation_GetOrInit(t *testing.T) {
	require := require.New(t)

	location := NewLocation(memfs.New())

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

	iter, err := NewLocationIterator(fs)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(id borges.RepositoryID, _ *borges.Repository) error {
		ids = append(ids, id)
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
	createValidDotGit(require, fs, "foo/bar")
	fs.MkdirAll("qux", 0755)
	createValidDotGit(require, fs, "qux/bar/baz/.git")
	createValidDotGit(require, fs, "qux/bar/.git")

	iter, err := NewLocationIterator(fs)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(id borges.RepositoryID, _ *borges.Repository) error {
		ids = append(ids, id)
		return nil
	})

	fmt.Println(ids)
	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"foo/qux", "foo/bar", "qux/bar",
	})
}
