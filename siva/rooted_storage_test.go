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
	testRootedIterators(t, borges.ReadOnlyMode)
	testRootedIterators(t, borges.RWMode)
}

func testRootedIterators(t *testing.T, mode borges.Mode) {
	t.Helper()

	options := LibraryOptions{
		RootedRepo: true,
	}

	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)
	lib, err := NewLibrary("rooted", fs, options)
	require.NoError(t, err)

	tests := []struct {
		name    string
		commits []string
		trees   []string
		blobs   []string
	}{
		{
			name: "gitserver.com/a",
			commits: []string{
				"e09387d4fb5e8ac82494955d03733a63f1936cd9",
				"4debba8a88e808bdef8364026db890c5cb2900de",
				"cf2e799463e1a00dbd1addd2003b0c7db31dbfe2",
			},
			trees: []string{
				"668bd3eb5acf5321d3300b074afb7d281023a1ce",
				"acdcd507f9f9453679a769296f1606d08a4ab3dc",
				"764409de08fa4fda9ba6c85a54f5f31d00cec93e",
			},
			blobs: []string{
				"d9d9859f9bed4e254861ab9f898d38da7ef31ca2",
				"06c773547c9d0dfd32c349dfd142309b631cd42a",
				"8178c76d627cade75005b40711b92f4177bc6cfc",
			},
		},
		{
			name: "gitserver.com/b",
			commits: []string{
				"0c17762a2c24b2e9c01aea9ba3dc15e5176e68da",
				"4debba8a88e808bdef8364026db890c5cb2900de",
				"cf2e799463e1a00dbd1addd2003b0c7db31dbfe2",
				"8c46128f7f8dca511321eb58940da6138a42ab42",
				"e09387d4fb5e8ac82494955d03733a63f1936cd9",
			},
			trees: []string{
				"1ff31a3be40580a5a668635f8866d1a4be4b4bbe",
				"acdcd507f9f9453679a769296f1606d08a4ab3dc",
				"764409de08fa4fda9ba6c85a54f5f31d00cec93e",
				"285ad25aba9f4b214e2434b87bf3320ffad42329",
				"668bd3eb5acf5321d3300b074afb7d281023a1ce",
			},
			blobs: []string{
				"9ca8e84e39ee77115d5c13d942ab5834fd565acd",
				"06c773547c9d0dfd32c349dfd142309b631cd42a",
				"8178c76d627cade75005b40711b92f4177bc6cfc",
				"773b2222280159571c11f3dea41023dbfaabf5c6",
				"d9d9859f9bed4e254861ab9f898d38da7ef31ca2",
			},
		},
		{
			name: "gitserver.com/c",
			commits: []string{
				"d47421c1ab4ab5d2e00ba5f0bbeccf9578dd3d5c",
				"75129d3d3cc746b4cd335f9c01f1dad21d865403",
				"8c46128f7f8dca511321eb58940da6138a42ab42",
				"4debba8a88e808bdef8364026db890c5cb2900de",
				"cf2e799463e1a00dbd1addd2003b0c7db31dbfe2",
				"0c17762a2c24b2e9c01aea9ba3dc15e5176e68da",
			},
			trees: []string{
				"86e1673eae32bb2cb9d36a19b5f312d62519697b",
				"ea7e17f6ea1448e56aa092164a60e509fc59170d",
				"285ad25aba9f4b214e2434b87bf3320ffad42329",
				"acdcd507f9f9453679a769296f1606d08a4ab3dc",
				"764409de08fa4fda9ba6c85a54f5f31d00cec93e",
				"1ff31a3be40580a5a668635f8866d1a4be4b4bbe",
			},
			blobs: []string{
				"06c773547c9d0dfd32c349dfd142309b631cd42a",
				"773b2222280159571c11f3dea41023dbfaabf5c6",
				"ee02d961b370c701629363409d07afc3da5e26dc",
				"dac138d9e013a2e9a10e67d793bd4703c1b86bd1",
				"8178c76d627cade75005b40711b92f4177bc6cfc",
				"9ca8e84e39ee77115d5c13d942ab5834fd565acd",
			},
		},
		{
			name: "gitserver.com/d",
			commits: []string{
				"86d4bd99755baf332361cf364a9772b74fcb64d6",
				"e7a760958a2d664295f0465bf4ca979926dc8550",
				"7999f9666861c0fc1d310cf9d7d03420ff29b0a8",
				"d47421c1ab4ab5d2e00ba5f0bbeccf9578dd3d5c",
				"75129d3d3cc746b4cd335f9c01f1dad21d865403",
				"8c46128f7f8dca511321eb58940da6138a42ab42",
				"4debba8a88e808bdef8364026db890c5cb2900de",
				"cf2e799463e1a00dbd1addd2003b0c7db31dbfe2",
				"589e548bfa67701f846000dd238e2660b2067284",
				"5c016197dc226e105f3b5edcd259d7cfb0516a89",
				"dded5ecdd46f6b3557183022f69b55a428bb377d",
				"47055f43258f5f8e4910ba23755e5e9f5a7413a9",
				"c9d7d3f6b8e482d1dff8a31c4f2e8abc23aa8d90",
				"9a87f5f16e1c14d7493c138c7f8783ba6d359b7f",
			},
			trees: []string{
				"34c41faf8813eda3f42cea23ec20432e281f5dff",
				"4282a0ee2f81c6dd9fdb4b3e10cffc5ff7948ac6",
				"9a2a0b1379ba12472bc0e1070a8f0787a69310e8",
				"13159d380a0edb7745e272f60caa7d6703f0067e",
				"425534a90f90ed43be88932b56a420d5be6c9481",
				"6e1021a51be9becce6964744cb6fadb88328dfd0",
				"86e1673eae32bb2cb9d36a19b5f312d62519697b",
				"ea7e17f6ea1448e56aa092164a60e509fc59170d",
				"285ad25aba9f4b214e2434b87bf3320ffad42329",
				"acdcd507f9f9453679a769296f1606d08a4ab3dc",
				"764409de08fa4fda9ba6c85a54f5f31d00cec93e",
				"5126b1309620918bca11af91551b5e6824c06395",
				"1ecbc5095fa42400d6b707168e3f3656cdda4c54",
				"3a51818b3c211066b717b052c65301f52c767335",
				"16f832b9631638e0091ebe8d821aca6a877ea8db",
				"2e1ad421ec80296c4085a920318850f9f1b26695",
				"af714a60782eb9b115d07722162e923a89462b0f",
				"971a644ef767f49a06438eb685835a2e57f3bdfe",
				"ac623ec7cb810c88ec0c7721fe0def08b2ed773c",
				"7b2272cb6464b9dbacbe00a767d2874465a860d3",
			},
			blobs: []string{
				"06c773547c9d0dfd32c349dfd142309b631cd42a",
				"dac138d9e013a2e9a10e67d793bd4703c1b86bd1",
				"773b2222280159571c11f3dea41023dbfaabf5c6",
				"ee02d961b370c701629363409d07afc3da5e26dc",
				"8178c76d627cade75005b40711b92f4177bc6cfc",
				"8da84891bfc9327c9d079dc09cd5f84be307d8f3",
				"c6cac69265af1e1684d2e3038f8fc90b84c87e9c",
				"8e27be7d6154a1f68ea9160ef0e18691d20560dc",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := borges.RepositoryID(test.name)
			repo, err := lib.Get(id, mode)
			require.NoError(t, err)
			defer repo.Close()

			r := repo.R()

			// commits

			iter, err := r.Storer.IterEncodedObjects(plumbing.CommitObject)
			require.NoError(t, err)

			var commits []string
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				commits = append(commits, obj.Hash().String())

				require.Equal(t, plumbing.CommitObject, obj.Type())
				return nil
			})
			require.NoError(t, err)
			require.ElementsMatch(t, test.commits, commits,
				"the number of commits is incorrect")

			// trees

			iter, err = r.Storer.IterEncodedObjects(plumbing.TreeObject)
			require.NoError(t, err)

			var trees []string
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				trees = append(trees, obj.Hash().String())

				require.Equal(t, plumbing.TreeObject, obj.Type(),
					"object type is incorrect")
				return nil
			})
			require.NoError(t, err)
			require.ElementsMatch(t, test.trees, trees,
				"the number of trees is incorrect")

			// blobs

			iter, err = r.Storer.IterEncodedObjects(plumbing.BlobObject)
			require.NoError(t, err)

			var blobs []string
			err = iter.ForEach(func(obj plumbing.EncodedObject) error {
				blobs = append(blobs, obj.Hash().String())

				require.Equal(t, plumbing.BlobObject, obj.Type(),
					"object type is incorrect")
				return nil
			})
			require.NoError(t, err)
			require.ElementsMatch(t, test.blobs, blobs,
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
