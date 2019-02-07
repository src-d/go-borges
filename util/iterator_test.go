package util_test

import (
	"io"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
	"github.com/stretchr/testify/require"
)

func TestNewLocationRepositoryIteratorNext(t *testing.T) {
	require := require.New(t)

	iter := util.NewLocationRepositoryIterator(nil, borges.RWMode)
	r, err := iter.Next()
	require.Equal(err, io.EOF)
	require.Nil(r)
}
