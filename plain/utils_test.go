package plain

import (
	"testing"

	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
)

func TestIsRepository(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()

	is, bare, err := IsRepository(fs, "foo")
	require.NoError(err)
	require.False(is)
	require.False(bare)

	createValidDotGit(require, fs, "foo/.git")

	is, bare, err = IsRepository(fs, "foo")
	require.NoError(err)
	require.True(is)
	require.False(bare)

}

func TestIsRepository_Bare(t *testing.T) {
	require := require.New(t)

	fs := memfs.New()

	is, bare, err := IsRepository(fs, "foo")
	require.NoError(err)
	require.False(is)
	require.False(bare)

	createValidDotGit(require, fs, "foo")

	is, bare, err = IsRepository(fs, "foo")
	require.NoError(err)
	require.True(is)
	require.True(bare)
}

func createValidDotGit(require *require.Assertions, fs billy.Filesystem, path string) {
	_, err := fs.Create(fs.Join(path, "HEAD"))
	require.NoError(err)

	err = fs.MkdirAll(fs.Join(path, "objects"), 0755)
	require.NoError(err)

	err = fs.MkdirAll(fs.Join(path, "refs", "heads"), 0755)
	require.NoError(err)
}
