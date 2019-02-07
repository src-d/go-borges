package plain

import (
	"os"
	"testing"

	"github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestInitRepository(t *testing.T) {
	require := require.New(t)

	memory := memfs.New()
	location, err := NewLocation("foo", memory, nil)
	require.NoError(err)

	r, err := initRepository(location, "github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	remote, err := r.R().Remote("origin")
	require.NoError(err)
	require.NotNil(remote)

	_, err = memory.Stat("github.com/foo/bar/.git")
	require.NoError(err)

}

func TestInitRepository_Transactional(t *testing.T) {
	require := require.New(t)

	memory := memfs.New()
	tmp := memfs.New()

	location, err := NewLocation("foo", memory, &LocationOptions{
		Transactional:      true,
		TemporalFilesystem: tmp,
	})
	require.NoError(err)

	r, err := initRepository(location, "github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	remote, err := r.R().Remote("origin")
	require.NoError(err)
	require.NotNil(remote)

	_, err = memory.Stat("github.com/foo/bar/.git")
	require.True(os.IsNotExist(err))

	_, err = tmp.Stat(tmp.Join(r.temporalPath, "config"))
	require.NoError(err)
}

func TestOpenRepository(t *testing.T) {
	require := require.New(t)

	location := newLocationWithFixtures(require, nil)

	r, err := openRepository(location, "basic.git", borges.RWMode)
	require.NoError(err)
	require.NotNil(r)

	remote, err := r.R().Remote("origin")
	require.NoError(err)
	require.Equal("origin", remote.Config().Name)
}

func TestRepository_Commit_OnNonTransactional(t *testing.T) {
	require := require.New(t)

	location := newLocationWithFixtures(require, nil)

	r, err := location.Get("basic.git", borges.RWMode)
	require.NoError(err)

	err = r.Commit()
	require.True(borges.ErrNonTransactional.Is(err))
}

func TestRepository_Close(t *testing.T) {
	require := require.New(t)
	tmp := memfs.New()

	location := newLocationWithFixtures(require, &LocationOptions{
		Bare:               true,
		Transactional:      true,
		TemporalFilesystem: tmp,
	})

	r, err := location.Get("basic.git", borges.RWMode)
	require.NoError(err)

	r.R().Storer.SetReference(plumbing.NewHashReference("refs/heads/foo", plumbing.ZeroHash))

	_, err = tmp.Stat(tmp.Join(r.(*Repository).temporalPath, "refs/heads/foo"))
	require.NoError(err)

	err = r.Close()
	require.NoError(err)

	_, err = tmp.Stat(tmp.Join(r.(*Repository).temporalPath, "refs/heads/foo"))
	require.True(os.IsNotExist(err))
}
