package siva

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/util"
)

const testDir = "../_testdata/siva"

func setupFS(t *testing.T) (billy.Filesystem, []string) {
	t.Helper()
	require := require.New(t)

	entries, err := ioutil.ReadDir(testDir)
	require.NoError(err)

	var sivas []string
	for _, e := range entries {
		if e.Mode().IsRegular() &&
			strings.HasSuffix(e.Name(), ".siva") {
			sivas = append(sivas, e.Name())
		}
	}

	require.True(len(sivas) > 0,
		"siva files not found in test directory")

	fs := memfs.New()

	for _, siva := range sivas {
		path := filepath.Join(testDir, siva)
		sivaData, err := ioutil.ReadFile(path)
		require.NoError(err)
		err = util.WriteFile(fs, siva, sivaData, 0666)
		require.NoError(err)
	}

	return fs, sivas
}

func setupLibrary(
	t *testing.T,
	id string,
	opts LibraryOptions,
) *Library {
	t.Helper()
	var require = require.New(t)

	fs, _ := setupFS(t)
	lib, err := NewLibrary(id, fs, opts)
	require.NoError(err)

	return lib
}

func setupTranstaction(
	t *testing.T,
) (borges.Location, borges.Repository, borges.Repository) {
	t.Helper()
	require := require.New(t)

	lib := setupLibrary(t, "test", LibraryOptions{
		Transactional: true,
	})

	l, err := lib.Location("foo-bar")
	require.NoError(err)

	// open two repositories, the write one is in transaction mode
	r, err := l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)
	w, err := l.Get("github.com/foo/bar", borges.RWMode)
	require.NoError(err)

	return l, r, w
}
