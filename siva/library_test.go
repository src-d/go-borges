package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/test"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func TestLibrary(t *testing.T) {
	s := new(test.LibrarySuite)
	fs := osfs.New("../_testdata/siva")

	s.LibrarySingle = func() borges.Library {
		lib, err := NewLibrary("foo", fs, LibraryOptions{})
		require.NoError(t, err)

		return lib
	}

	suite.Run(t, s)
}
