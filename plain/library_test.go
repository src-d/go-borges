package plain

import (
	"fmt"
	"io"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/test"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func newLibrary(s suite.Suite, name string) *Library {
	require := s.Require()

	idqux := borges.LocationID(fmt.Sprintf("%s-qux", name))
	lqux, _ := NewLocation(idqux, memfs.New(), nil)
	idbar := borges.LocationID(fmt.Sprintf("%s-bar", name))
	lbar, _ := NewLocation(idbar, memfs.New(), nil)

	l := NewLibrary(borges.LibraryID(name))
	l.AddLocation(lqux)
	l.AddLocation(lbar)

	nqux := borges.RepositoryID(fmt.Sprintf("github.com/%s/qux", name))
	_, err := lqux.Init(nqux)
	require.NoError(err)

	nbar := (borges.RepositoryID(fmt.Sprintf("github.com/%s/bar", name)))
	_, err = lbar.Init(nbar)
	require.NoError(err)

	return l
}

func TestLibrary(t *testing.T) {
	s := new(test.LibraryNestedSuite)
	s.LibrarySingle = func() borges.Library {
		return newLibrary(s.Suite, "foo")
	}
	s.LibraryNested = func() borges.Library {
		baz := NewLibrary("baz")

		foo := newLibrary(s.Suite, "foo")
		baz.AddLibrary(foo)

		nested := NewLibrary("nested")
		deep := newLibrary(s.Suite, "deep")
		nested.AddLibrary(deep)
		baz.AddLibrary(nested)

		return baz
	}

	suite.Run(t, s)
}

func TestLibraryRepositoriesError(t *testing.T) {
	require := require.New(t)
	idqux := borges.LocationID("qux")
	lqux, _ := NewLocation(idqux, osfs.New("/does/not/exist/qux"), nil)
	idbar := borges.LocationID("bar")
	lbar, _ := NewLocation(idbar, osfs.New("/does/not/exist/bar"), nil)
	idbaz := borges.LocationID("baz")
	lbaz, _ := NewLocation(idbaz, memfs.New(), nil)

	l := NewLibrary(borges.LibraryID("broken"))
	l.AddLocation(lqux)
	l.AddLocation(lbar)
	l.AddLocation(lbaz)

	nbaz := borges.RepositoryID("github.com/source/bar")
	_, err := lbaz.Init(nbaz)
	require.NoError(err)

	it, err := l.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var errors int
	var repos int
	var count int
	for {
		repo, err := it.Next()
		if err == io.EOF {
			break
		}

		if err == nil {
			repo.Close()
			repos++
		} else {
			errors++
		}

		count++

		if count > 3 {
			break
		}
	}

	require.Equal(3, count)
	require.Equal(2, errors)
	require.Equal(1, repos)
}
