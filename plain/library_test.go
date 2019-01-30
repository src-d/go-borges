package plain

import (
	"testing"

	"github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
)

func TestLibrary(t *testing.T) {
	require := require.New(t)

	var library borges.Library
	library = NewLibrary("foo")
	require.NotNil(library)
	require.Equal(library.ID(), borges.LibraryID("foo"))
}

func TestLibrary_Has(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	ok, lib, loc, err := l.Has("http://github.com/foo/qux")
	require.NoError(err)
	require.True(ok)
	require.Equal(borges.LibraryID("foo"), lib)
	require.Equal(borges.LocationID("bar"), loc)
}

func TestLibrary_Has_NestedLibrary(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	library := NewLibrary("baz")
	library.AddLibrary(l)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	ok, lib, loc, err := library.Has("http://github.com/foo/qux")
	require.NoError(err)
	require.True(ok)
	require.Equal(borges.LibraryID("foo"), lib)
	require.Equal(borges.LocationID("bar"), loc)
}

func TestLibrary_Get(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := l.Get("http://github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("bar"), r.LocationID())
}

func TestLibrary_Get_NestedLibrary(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	library := NewLibrary("baz")
	library.AddLibrary(l)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := library.Get("http://github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("bar"), r.LocationID())
}

func TestLibrary_Get_DeepNestedLibrary(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	l2 := NewLibrary("baz")
	l2.AddLibrary(l)

	library := NewLibrary("qux")
	library.AddLibrary(l2)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := library.Get("http://github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.LocationID("bar"), r.LocationID())
}

func TestLibrary_Get_NotFound(t *testing.T) {
	require := require.New(t)

	l := NewLibrary("foo")
	r, err := l.Get("http://github.com/foo/qux", borges.RWMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))
	require.Nil(r)
}

func TestLibrary_Location(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)

	r, err := l.Location("foo")
	require.NoError(err)
	require.NotNil(r)
}

func TestLibrary_Location_NotFound(t *testing.T) {
	require := require.New(t)

	l := NewLibrary("foo")
	r, err := l.Location("foo")
	require.True(borges.ErrLocationNotExists.Is(err))
	require.Nil(r)
}

func TestLibrary_Library(t *testing.T) {
	require := require.New(t)

	l := NewLibrary("foo")
	l.AddLibrary(NewLibrary("bar"))

	r, err := l.Library("bar")
	require.NoError(err)
	require.NotNil(r)
}

func TestLibrary_Library_NotFound(t *testing.T) {
	require := require.New(t)

	l := NewLibrary("foo")
	r, err := l.Library("bar")
	require.True(borges.ErrLibraryNotExists.Is(err))
	require.Nil(r)
}

func TestLibrary_Repositories(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary("foo")
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	_, err := lbar.Init("foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("foo/bar")
	require.NoError(err)

	iter, err := l.Repositories(borges.RWMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	err = iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	})

	require.NoError(err)
	require.ElementsMatch(ids, []borges.RepositoryID{
		"foo/qux",
		"foo/bar",
	})
}
