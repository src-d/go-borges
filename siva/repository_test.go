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
       fs := setupFS(t)

       lib, err := NewLibrary("test", fs, LibraryOptions{
               Transactional: transactional,
       })
       require.NoError(err)

       r, err := lib.Get("github.com/foo/bar", borges.ReadOnlyMode)
       require.NoError(err)

       _, err = r.R().CreateTag("tag", plumbing.ZeroHash, nil)
       require.True(util.ErrReadOnlyStorer.Is(err))
}


