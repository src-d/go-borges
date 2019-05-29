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

const (
	testDir       = "../_testdata/siva"
	testRootedDir = "../_testdata/rooted"
)

func setupMemFS(t *testing.T, bucket int) (billy.Filesystem, []string) {
	t.Helper()
	return setupFS(t, testDir, true, bucket)
}

func setupOSFS(t *testing.T, bucket int) (billy.Filesystem, []string) {
	t.Helper()
	return setupFS(t, testDir, false, bucket)
}

func setupFS(t *testing.T, dir string, inMem bool, bucket int) (billy.Filesystem, []string) {
	t.Helper()
	require := require.New(t)

	entries, err := ioutil.ReadDir(dir)
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

	for _, testSiva := range sivas {
		path := filepath.Join(dir, testSiva)
		data, err := ioutil.ReadFile(path)
		require.NoError(err)

		siva := testSiva
		id := toLocID(siva)
		siva = buildSivaPath(id, bucket)
		err = util.WriteFile(fs, siva, data, 0666)
		require.NoError(err)

		bucketNoise := []int{0, 1, 2, 100}
		for _, b := range bucketNoise {
			if b == bucket {
				continue
			}

			altSiva := buildSivaPath(id, b)
			err := util.WriteFile(fs, altSiva, []byte{}, 0666)
			require.NoError(err)
		}
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

	fs, _ := setupMemFS(t, opts.Bucket)
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
