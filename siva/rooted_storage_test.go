package siva

import (
	"testing"

	"github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestRootedIterateReferences(t *testing.T) {
	options := LibraryOptions{
		RootedRepo: true,
	}

	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)
	lib, err := NewLibrary("rooted", fs, options)
	require.NoError(t, err)

	tests := []struct {
		name     string
		expected []*plumbing.Reference
	}{
		{
			name: "gitserver.com/a",
			expected: []*plumbing.Reference{
				hr("refs/heads/fix", "e09387d4fb5e8ac82494955d03733a63f1936cd9"),
				hr("refs/heads/master", "4debba8a88e808bdef8364026db890c5cb2900de"),
				// now symbolic references are converted to hash references
				// sr("HEAD", "refs/heads/fix"),
				hr("HEAD", "e09387d4fb5e8ac82494955d03733a63f1936cd9"),
			},
		},
		{
			name: "gitserver.com/b",
			expected: []*plumbing.Reference{
				hr("refs/heads/fix", "0c17762a2c24b2e9c01aea9ba3dc15e5176e68da"),
				hr("refs/heads/master", "8c46128f7f8dca511321eb58940da6138a42ab42"),
				// now symbolic references are converted to hash references
				// sr("HEAD", "refs/heads/master"),
				hr("HEAD", "8c46128f7f8dca511321eb58940da6138a42ab42"),

				// remotes
				hr("refs/remotes/origin/fix", "e09387d4fb5e8ac82494955d03733a63f1936cd9"),
				hr("refs/remotes/origin/master", "4debba8a88e808bdef8364026db890c5cb2900de"),
				hr("refs/remotes/origin/HEAD", "4debba8a88e808bdef8364026db890c5cb2900de"),
			},
		},
		{
			name: "gitserver.com/c",
			expected: []*plumbing.Reference{
				hr("refs/heads/css", "d47421c1ab4ab5d2e00ba5f0bbeccf9578dd3d5c"),
				hr("refs/heads/master", "75129d3d3cc746b4cd335f9c01f1dad21d865403"),
				// now symbolic references are converted to hash references
				// sr("HEAD", "refs/heads/css"),
				hr("HEAD", "d47421c1ab4ab5d2e00ba5f0bbeccf9578dd3d5c"),

				// remotes
				hr("refs/remotes/origin/fix", "0c17762a2c24b2e9c01aea9ba3dc15e5176e68da"),
				hr("refs/remotes/origin/master", "8c46128f7f8dca511321eb58940da6138a42ab42"),
				hr("refs/remotes/origin/HEAD", "8c46128f7f8dca511321eb58940da6138a42ab42"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := borges.RepositoryID(test.name)
			repo, err := lib.Get(id, borges.ReadOnlyMode)
			require.NoError(t, err)
			defer repo.Close()

			r := repo.R()
			iter, err := r.References()
			require.NoError(t, err)

			var refs []*plumbing.Reference
			err = iter.ForEach(func(r *plumbing.Reference) error {
				refs = append(refs, r)
				return nil
			})
			require.NoError(t, err)

			require.ElementsMatch(t, refs, test.expected)
		})
	}
}

func TestRootedSetReference(t *testing.T) {
	require := require.New(t)

	options := LibraryOptions{
		RootedRepo: true,
	}

	fs, _ := setupFS(t, "../_testdata/rooted", false, 0)
	lib, err := NewLibrary("rooted", fs, options)
	require.NoError(err)

	repo, err := lib.Get("gitserver.com/a", borges.RWMode)
	require.NoError(err)

	testRef := hr("refs/heads/test", "4debba8a88e808bdef8364026db890c5cb2900de")
	checkRef := hr("refs/heads/check", "e09387d4fb5e8ac82494955d03733a63f1936cd9")

	r := repo.R()

	_, err = r.Reference(testRef.Name(), false)
	require.Equal(plumbing.ErrReferenceNotFound, err)
	_, err = r.Reference(checkRef.Name(), false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	err = r.Storer.CheckAndSetReference(testRef, checkRef)
	require.NoError(err)

	_, err = r.Reference(testRef.Name(), false)
	require.NoError(err)
	_, err = r.Reference(checkRef.Name(), false)
	require.Equal(plumbing.ErrReferenceNotFound, err)

	err = r.Storer.SetReference(checkRef)
	require.NoError(err)
	_, err = r.Reference(checkRef.Name(), false)
	require.NoError(err)

	err = repo.Close()
	require.NoError(err)

	options = LibraryOptions{
		RootedRepo: false,
	}

	lib, err = NewLibrary("rooted", fs, options)
	require.NoError(err)

	repo, err = lib.Get("gitserver.com/a", borges.ReadOnlyMode)
	require.NoError(err)
	r = repo.R()

	ref, err := r.Reference("refs/remotes/gitserver.com/a/heads/test", false)
	require.NoError(err)
	require.Equal("4debba8a88e808bdef8364026db890c5cb2900de", ref.Hash().String())

	ref, err = r.Reference("refs/remotes/gitserver.com/a/heads/check", false)
	require.NoError(err)
	require.Equal("e09387d4fb5e8ac82494955d03733a63f1936cd9", ref.Hash().String())

	err = repo.Close()
	require.NoError(err)
}

func TestRootedIterateObjects(t *testing.T) {
	options := LibraryOptions{
		RootedRepo: true,
	}

	fs, _ := setupFS(t, "../_testdata/rooted", true)
	lib, err := NewLibrary("rooted", fs, options)
	require.NoError(t, err)

	tests := []struct {
		name    string
		commits int
		trees   int
		blobs   int
	}{
		{
			name:    "gitserver.com/a",
			commits: 3,
			trees:   3,
			blobs:   3,
		},
		{
			name:    "gitserver.com/b",
			commits: 5,
			trees:   5,
			blobs:   5,
		},
		{
			name:    "gitserver.com/c",
			commits: 6,
			trees:   6,
			blobs:   6,
		},
		{
			name:    "gitserver.com/d",
			commits: 14,
			trees:   20,
			blobs:   8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := borges.RepositoryID(test.name)
			repo, err := lib.Get(id, borges.ReadOnlyMode)
			require.NoError(t, err)
			defer repo.Close()

			r := repo.R()

			// commits

			iter, err := r.Storer.IterEncodedObjects(plumbing.CommitObject)
			require.NoError(t, err)

			var commits int
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				commits++

				require.Equal(t, plumbing.CommitObject, obj.Type())
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.commits, commits,
				"the number of commits is incorrect")

			// trees

			iter, err = r.Storer.IterEncodedObjects(plumbing.TreeObject)
			require.NoError(t, err)

			var trees int
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				trees++

				require.Equal(t, plumbing.TreeObject, obj.Type(),
					"object type is incorrect")
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.trees, trees,
				"the number of trees is incorrect")

			// blobs

			iter, err = r.Storer.IterEncodedObjects(plumbing.BlobObject)
			require.NoError(t, err)

			var blobs int
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				blobs++

				require.Equal(t, plumbing.BlobObject, obj.Type(),
					"object type is incorrect")
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, test.blobs, blobs,
				"the number of blobs is incorrect")
		})
	}
}

func hr(n, h string) *plumbing.Reference {
	return plumbing.NewHashReference(
		plumbing.ReferenceName(n),
		plumbing.NewHash(h))
}

func sr(n, t string) *plumbing.Reference {
	return plumbing.NewSymbolicReference(
		plumbing.ReferenceName(n),
		plumbing.ReferenceName(t))
}
