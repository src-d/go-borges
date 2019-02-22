package siva

import (
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestReadOnly(t *testing.T) {
	testReadOnly(t, false)
}

func TestReadOnlyTransactional(t *testing.T) {
	testReadOnly(t, true)
}

func testReadOnly(t *testing.T, transactional bool) {
	t.Helper()
	require := require.New(t)

	lib := setupLibrary(t, "test", LibraryOptions{
		Transactional: transactional,
	})

	r, err := lib.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	_, err = r.R().CreateTag("tag", plumbing.ZeroHash, nil)
	require.True(util.ErrReadOnlyStorer.Is(err))
}
