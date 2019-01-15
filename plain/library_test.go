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
	library = NewLibrary()
	require.NotNil(library)
}

func TestLibrary_Has(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary()
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	ok, location, err := l.Has("http://github.com/foo/qux")
	require.NoError(err)
	require.True(ok)
	require.Equal(borges.MustLocationID("bar"), location)
}

func TestLibrary_Get(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)
	lbar, _ := NewLocation("bar", memfs.New(), nil)

	l := NewLibrary()
	l.AddLocation(lfoo)
	l.AddLocation(lbar)

	_, err := lbar.Init("http://github.com/foo/qux")
	require.NoError(err)

	_, err = lfoo.Init("http://github.com/foo/bar")
	require.NoError(err)

	r, err := l.Get("http://github.com/foo/qux", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	require.Equal(borges.MustLocationID("bar"), r.LocationID)
}

func TestLibrary_Get_NotFound(t *testing.T) {
	require := require.New(t)

	l := NewLibrary()
	r, err := l.Get("http://github.com/foo/qux", borges.RWMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))
	require.Nil(r)
}

func TestLocation_Location(t *testing.T) {
	require := require.New(t)

	lfoo, _ := NewLocation("foo", memfs.New(), nil)

	l := NewLibrary()
	l.AddLocation(lfoo)

	r, err := l.Location("foo")
	require.NoError(err)
	require.NotNil(r)
}

func TestLocation_Location_NotFound(t *testing.T) {
	require := require.New(t)

	l := NewLibrary()
	r, err := l.Location("foo")
	require.True(borges.ErrLocationNotExists.Is(err))
	require.Nil(r)
}
