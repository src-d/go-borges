package siva

import (
	"io/ioutil"
	"path"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/util"
	git "gopkg.in/src-d/go-git.v4"
)

func setupFS(t *testing.T) billy.Filesystem {
	t.Helper()
	require := require.New(t)

	fs := memfs.New()

	sivaFiles := []string{
		"foo-bar.siva",
		"foo-qux.siva",
	}

	for _, f := range sivaFiles {
		path := path.Join("..", "_testdata", "siva", f)
		sivaData, err := ioutil.ReadFile(path)
		require.NoError(err)
		err = util.WriteFile(fs, f, sivaData, 0666)
		require.NoError(err)
	}

	return fs
}

func setupTranstaction(
	t *testing.T,
) (borges.Location, borges.Repository, borges.Repository) {
	t.Helper()
	require := require.New(t)

	fs := setupFS(t)

	lib, err := NewLibrary("test", fs, LibraryOptions{
		Transactional: true,
	})
	require.NoError(err)

	l, err := lib.Location("foo-bar")
	require.NoError(err)

	// open two repositories, the write one is in transaction mode
	r, err := l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)
	w, err := l.Get("github.com/foo/bar", borges.RWMode)
	require.NoError(err)

	return l, r, w
}

func TestCommit(t *testing.T) {
	require := require.New(t)
	l, r, w := setupTranstaction(t)

	read := r.R()
	write := w.R()

	head, err := read.Head()
	require.NoError(err)

	// a tag created in the write repo should not be seen in the read one

	_, err = write.CreateTag("new_tag", head.Hash(), nil)
	require.NoError(err)

	_, err = read.Tag("new_tag")
	require.Equal(git.ErrTagNotFound, err)

	tag, err := write.Tag("new_tag")
	require.NoError(err)
	require.Equal(head.Hash(), tag.Hash())

	// newly repositories opened before commit should see the previous state

	r, err = l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	_, err = r.R().Tag("new_tag")
	require.Equal(git.ErrTagNotFound, err)

	err = w.Commit()
	require.NoError(err)

	// after commit the repository should be marked as closed
	err = w.Commit()
	require.EqualError(err, ErrRepoAlreadyClosed.New(w.ID()).Error())

	// after commit the tag should still not be seen in the read repo

	_, err = read.Tag("new_tag")
	require.Equal(git.ErrTagNotFound, err)

	// open the repo again and check that the tag is there

	r, err = l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	_, err = r.R().Tag("new_tag")
	require.NoError(err)
}

func TestRollback(t *testing.T) {
	require := require.New(t)
	l, _, w := setupTranstaction(t)

	write := w.R()
	head, err := write.Head()
	require.NoError(err)

	_, err = write.CreateTag("new_tag", head.Hash(), nil)
	require.NoError(err)

	err = w.Close()
	require.NoError(err)

	// after colse the repository should be marked as closed
	err = w.Close()
	require.EqualError(err, ErrRepoAlreadyClosed.New(w.ID()).Error())

	r, err := l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	_, err = r.R().Tag("new_tag")
	require.Equal(git.ErrTagNotFound, err)
}
