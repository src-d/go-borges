package oldsiva

import (
	"path/filepath"
	"testing"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"github.com/stretchr/testify/require"
)

func TestLibrary(t *testing.T) {
	var req = require.New(t)

	lib := setupLibrary(t, "test", &LibraryOptions{
		Bucket: 2,
	})

	locIter, err := lib.Locations()
	req.NoError(err)

	var count int
	req.NoError(locIter.ForEach(func(loc borges.Location) error {
		count++
		return nil
	}))
	locIter.Close()
	req.True(count == 2)

	repoIter, err := lib.Repositories(borges.RWMode)
	req.NoError(err)

	count = 0
	req.NoError(repoIter.ForEach(func(r borges.Repository) error {
		count++
		req.Equal(borges.ReadOnlyMode, r.Mode())
		req.True(borges.ErrNonTransactional.Is(r.Commit()))
		req.NoError(r.Close())
		return nil
	}))
	repoIter.Close()
	req.True(count == 2)

	ids := []borges.LocationID{
		"3974996807a9f596cf25ac3a714995c24bb97e2c",
		"f2cee90acf3c6644d51a37057845b98ab1580932",
	}

	for _, id := range ids {
		t.Run("location_"+string(id), func(t *testing.T) {
			ok, _, locID, err := lib.Has(borges.RepositoryID(id))
			req.NoError(err)
			req.True(ok)
			req.True(locID == id)

			l, err := lib.Location(id)
			req.NoError(err)

			loc, ok := l.(*Location)
			req.True(ok)
			req.Equal(filepath.Base(loc.path), string(id)+".siva")

			_, err = loc.GetOrInit(borges.RepositoryID(id))
			req.True(borges.ErrNotImplemented.Is(err))

			_, err = loc.Init(borges.RepositoryID("foo"))
			req.True(borges.ErrNotImplemented.Is(err))

			ok, err = loc.Has(borges.RepositoryID(id))
			req.NoError(err)
			req.True(ok)

			_, err = loc.Get("foo", borges.ReadOnlyMode)
			req.True(borges.ErrRepositoryNotExists.Is(err))

			r, err := loc.Get(
				borges.RepositoryID(id),
				borges.RWMode,
			)
			req.NoError(err)
			req.Equal(id, r.LocationID())
			req.Equal(borges.RepositoryID(id), r.ID())
			req.NoError(r.Close())
		})
	}

	id := borges.RepositoryID("3974996807a9f596cf25ac3a714995c24bb97e2c")
	r, err := lib.Get(id, borges.ReadOnlyMode)
	req.NoError(err)

	commitIter, err := r.R().CommitObjects()
	req.NoError(err)

	count = 0
	req.NoError(commitIter.ForEach(func(c *object.Commit) error {
		count++
		return nil
	}))
	commitIter.Close()
	req.Equal(13, count)
	req.NoError(r.Close())

	id = borges.RepositoryID("f2cee90acf3c6644d51a37057845b98ab1580932")
	r, err = lib.Get(id, borges.ReadOnlyMode)
	req.NoError(err)

	commitIter, err = r.R().CommitObjects()
	req.NoError(err)

	count = 0
	req.NoError(commitIter.ForEach(func(c *object.Commit) error {
		count++
		return nil
	}))
	commitIter.Close()
	req.Equal(368, count)
	req.NoError(r.Close())
}
