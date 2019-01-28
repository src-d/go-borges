package plain

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git-fixtures.v3"
)

func TestIsRepository(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()

	is, err := IsRepository(fs, "foo", false)
	require.NoError(err)
	require.False(is)

	createValidDotGit(require, fs, "foo/.git")

	is, err = IsRepository(fs, "foo", false)
	require.NoError(err)
	require.True(is)
}

func TestIsRepository_Bare(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()

	is, err := IsRepository(fs, "foo", true)
	require.NoError(err)
	require.False(is)

	createValidDotGit(require, fs, "foo")

	is, err = IsRepository(fs, "foo", true)
	require.NoError(err)
	require.True(is)
}

func createValidDotGit(require *require.Assertions, fs billy.Filesystem, path string) {
	_, err := fs.Create(fs.Join(path, "HEAD"))
	require.NoError(err)

	err = fs.MkdirAll(fs.Join(path, "objects"), 0755)
	require.NoError(err)

	err = fs.MkdirAll(fs.Join(path, "refs", "heads"), 0755)
	require.NoError(err)
}

func extractFixture(require *require.Assertions, f *fixtures.Fixture, path string) {
	err := os.Rename(f.DotGit().Root(), path)
	require.NoError(err)
}

func newLocationWithFixtures(require *require.Assertions, opts *LocationOptions) *Location {
	fixtures.Init()

	if opts == nil {
		opts = &LocationOptions{}
	}

	opts.Bare = true

	dir, err := ioutil.TempDir("", "location")
	require.NoError(err)

	extractFixture(require, fixtures.Basic().One(), filepath.Join(dir, "basic.git"))

	location, err := NewLocation("foo", osfs.New(dir), opts)
	require.NoError(err)

	return location
}
