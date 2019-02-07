package plain

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git-fixtures.v3"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestLocation(t *testing.T) {
	require := require.New(t)

	id, err := borges.NewRepositoryID("http://github.com/foo/bar")
	require.NoError(err)

	var location borges.Location
	location, err = NewLocation("foo", memfs.New(), nil)
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
		"github.com/foo/bar.git",
	})
}

func TestLocation_Has(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.False(has)
}

func TestLocation_RepositoryPath(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	path := location.RepositoryPath("github.com/foo/bar")
	require.Equal("github.com/foo/bar/.git", path)
}

func TestLocation_RepositoryPath_IsBare(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), &LocationOptions{Bare: true})
	require.NoError(err)

	path := location.RepositoryPath("github.com/foo/bar")
	require.Equal("github.com/foo/bar", path)
}

func TestLocation_Init(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	has, err := location.Has("http://github.com/foo/bar")
	require.NoError(err)
	require.True(has)
}

func TestLocation_InitExists(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	r, err = location.Init("http://github.com/foo/bar")
	require.True(borges.ErrRepositoryExists.Is(err))
	require.Nil(r)
}

func TestLocation_Get(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	_, err = location.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/bar", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("foo"), r.LocationID())
}

func TestLocation_Get_NotFound(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/qux", borges.RWMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))
	require.Nil(r)
}

func TestLocation_Get_ReadOnlyMode(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), nil)
	require.NoError(err)

	_, err = location.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := location.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)
	require.NotNil(r)

	err = r.R().Storer.SetReference(plumbing.NewHashReference("foo", plumbing.ZeroHash))
	require.True(util.ErrReadOnlyStorer.Is(err))
}

func TestLocation_Get_Transactional(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), &LocationOptions{
		Transactional: true,
	})
	require.NoError(err)

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	h := plumbing.NewHash("434611b74cb54538088c6aeed4ed27d3044064fa")
	err = r.R().Storer.SetReference(plumbing.NewHashReference("refs/heads/foo", h))
	require.NoError(err)

	err = r.Commit()
	require.NoError(err)

	r, err = location.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	ref, err := r.R().Storer.Reference("refs/heads/foo")
	require.NoError(err)
	require.Equal(ref.Hash(), h)
}

func TestLocation_GetOrInit(t *testing.T) {
	require := require.New(t)

	location, err := NewLocation("foo", memfs.New(), &LocationOptions{Bare: true})
	require.NoError(err)

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

	location, err := NewLocation("foo", fs, nil)
	require.NoError(err)

	iter, err := NewLocationIterator(location, borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.Len(ids, 1)
	require.Equal(ids[0].String(), "foo")
}

func TestLocationIterator_Next_Fixture(t *testing.T) {
	require := require.New(t)

	fixtures.Init()

	dir, err := ioutil.TempDir("", "location")
	require.NoError(err)
	extractFixture(require, fixtures.Basic().One(), filepath.Join(dir, "basic.git"))
	extractFixture(require, fixtures.Basic().One(), filepath.Join(dir, "basic-alt.git"))

	var location borges.Location
	location, err = NewLocation("foo", osfs.New(dir), &LocationOptions{Bare: true})
	require.NoError(err)

	iter, err := location.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"basic.git",
		"basic-alt.git",
	})
}

func TestLocationIterator_NextBare(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo")
	fs.MkdirAll("qux", 0755)

	location, err := NewLocation("foo", fs, &LocationOptions{Bare: true})
	require.NoError(err)

	iter, err := NewLocationIterator(location, borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.Len(ids, 1)
	require.Equal(ids[0].String(), "foo")
}

func TestLocationIterator_Next_DiferentLevels(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo/qux/.git")
	createValidDotGit(require, fs, "qux/bar/baz/.git")
	createValidDotGit(require, fs, "qux/baz/.git")

	location, err := NewLocation("foo", fs, nil)
	require.NoError(err)

	iter, err := NewLocationIterator(location, borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"foo/qux", "qux/bar/baz", "qux/baz",
	})
}

func TestLocationIterator_Next_Deep(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()
	createValidDotGit(require, fs, "foo/qux/.git")
	createValidDotGit(require, fs, "foo/bar/.git")
	fs.MkdirAll("qux", 0755)
	createValidDotGit(require, fs, "qux/bar/baz/.git")
	createValidDotGit(require, fs, "qux/bar/.git")

	location, err := NewLocation("foo", fs, nil)
	require.NoError(err)

	iter, err := NewLocationIterator(location, borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"foo/qux", "foo/bar", "qux/bar",
	})
}
