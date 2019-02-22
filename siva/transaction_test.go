package siva

import (
	"testing"
	"time"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	git "gopkg.in/src-d/go-git.v4"
)

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

func TestTwoInitNoCommit(t *testing.T) {
	require := require.New(t)

	lib := setupLibrary(t, "test", LibraryOptions{
		Transactional: true,
		Timeout:       100 * time.Millisecond,
	})

	location, err := lib.AddLocation("test")
	require.NoError(err)

	_, err = location.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))

	r, err := location.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	_, err = location.Init("http://github.com/foo/baz")
	require.Error(err)
	require.True(ErrTransactionTimeout.Is(err))
}
