package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/test"
	"github.com/stretchr/testify/suite"
)

func TestLibrary(t *testing.T) {
	s := new(test.LibrarySuite)
	s.LibrarySingle = func() borges.Library {
		return setupLibrary(t, "foo", LibraryOptions{})
	}

	suite.Run(t, s)
}
