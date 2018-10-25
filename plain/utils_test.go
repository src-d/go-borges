package plain

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
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
