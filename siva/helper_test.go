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
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

const testDir = "../_testdata/siva"

func setupMemFS(t *testing.T) (billy.Filesystem, []string) {
	t.Helper()
	return setupFS(t, true)
}

func setupOSFS(t *testing.T) (billy.Filesystem, []string) {
	t.Helper()
	return setupFS(t, false)
}

func setupFS(t *testing.T, inMem bool) (billy.Filesystem, []string) {
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

	var fs billy.Filesystem
	if inMem {
		fs = memfs.New()
	} else {
		path, err := ioutil.TempDir("", "go-borges-siva")
		require.NoError(err)

		fs = osfs.New(path)
	}

	for _, siva := range sivas {
		path := filepath.Join(testDir, siva)
		data, err := ioutil.ReadFile(path)
		require.NoError(err)
		err = util.WriteFile(fs, siva, data, 0666)
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

	fs, _ := setupMemFS(t)
	lib, err := NewLibrary(id, fs, opts)
	require.NoError(err)

	return lib
}

func createTagOnHead(
	t *testing.T,
	r borges.Repository,
	name string,
) *plumbing.Reference {
	t.Helper()
	var require = require.New(t)

	repo := r.R()
	require.NotNil(repo)

	head, err := repo.Head()
	require.NoError(err)

	_, err = repo.CreateTag(name, head.Hash(), nil)
	require.NoError(err)

	return head
}
