package plain

import (
	"fmt"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/test"

	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4/memfs"
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
