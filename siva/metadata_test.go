package siva

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestMetadataSiva(t *testing.T) {
	require := require.New(t)

	meta := &locationMetadata{
		Versions: map[int]*Version{
			0: &Version{
				Offset: 16,
				Size:   17,
			},
			1: &Version{
				Offset: 32,
				Size:   33,
			},
			10: &Version{
				Offset: 42,
				Size:   43,
			},
			20: &Version{
				Offset: 52,
				Size:   53,
			},
			2: &Version{
				Offset: 62,
				Size:   63,
			},
		},
	}

	data, err := yaml.Marshal(meta)
	require.NoError(err)

	m, err := parseLocationMetadata(data)
	require.NoError(err)
	require.EqualValues(*meta, *m)
}

func TestMetadataLibrary(t *testing.T) {
	const (
		rootedVersions = `
---
versions:
  "0":
    offset: 3180
  "1":
    offset: 6557
  "5":
    offset: 10296
  "6":
    offset: 17421
`

		metadata = `---
version: `
	)

	tests := []struct {
		version      int
		repositories []string
	}{
		{
			version: 0,
			repositories: []string{
				"gitserver.com/a",
			},
		},
		{
			version: 1,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
			},
		},
		{
			version: 2,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
			},
		},
		{
			version: 3,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
			},
		},
		{
			version: 4,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
			},
		},
		{
			version: 5,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
				"gitserver.com/c",
			},
		},
		{
			version: 6,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
				"gitserver.com/c",
				"gitserver.com/d",
			},
		},
		{
			version: -1,
			repositories: []string{
				"gitserver.com/a",
				"gitserver.com/b",
				"gitserver.com/c",
				"gitserver.com/d",
				"gitserver.com/e",
			},
		},
	}

	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)
	path := "cf2e799463e1a00dbd1addd2003b0c7db31dbfe2" + locMetadataFileExt
	err := util.WriteFile(fs, path, []byte(rootedVersions), 0666)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(fmt.Sprintf("version-%v", test.version), func(t *testing.T) {
			require := require.New(t)

			var lib *Library
			if test.version == -1 {
				// do not create metadata
				var err error
				lib, err = NewLibrary("test", fs, &LibraryOptions{
					MetadataReadOnly: true,
				})
				require.NoError(err)
			} else {
				libMd := metadata + strconv.Itoa(test.version)
				err := util.WriteFile(fs, libraryMetadataFile, []byte(libMd), 0666)
				require.NoError(err)
				defer func() {
					require.NoError(fs.Remove(libraryMetadataFile))
				}()

				lib, err = NewLibrary("test", fs, &LibraryOptions{})
				require.NoError(err)
			}

			v, err := lib.Version()
			require.NoError(err)
			require.Equal(test.version, v)

			it, err := lib.Repositories(borges.ReadOnlyMode)
			require.NoError(err)

			var repositories []string
			err = it.ForEach(func(r borges.Repository) error {
				repositories = append(repositories, r.ID().String())
				return nil
			})
			require.NoError(err)

			require.ElementsMatch(test.repositories, repositories)
		})
	}
}

func TestMetadataLibraryWrite(t *testing.T) {
	require := require.New(t)
	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

	// library does not have metadata in MetadataReadOnly mode
	lib, err := NewLibrary("test", fs, &LibraryOptions{
		MetadataReadOnly: true,
	})
	require.NoError(err)

	version, err := lib.Version()
	require.NoError(err)
	require.Equal(-1, version)

	// library creating metadata, since theres is no previous metadata
	// a new metadata file will be created with id "test"
	lib, err = NewLibrary("test", fs, &LibraryOptions{})
	require.NoError(err)

	_, err = fs.Stat(libraryMetadataFile)
	require.NoError(err, "library metadata file should exist")

	version, err = lib.Version()
	require.NoError(err)
	require.Equal(-1, version)
	require.Equal(borges.LibraryID("test"), lib.ID())

	require.NoError(lib.SetVersion(1))

	_, err = fs.Stat(libraryMetadataFile)
	require.NoError(err, "library metadata file should exist")

	// modify version and id in library metadata, since there is previous
	// metadata that is loaded the id will be ignored
	lib, err = NewLibrary("foo", fs, &LibraryOptions{})
	require.NoError(err)

	version, err = lib.Version()
	require.NoError(err)
	require.Equal(1, version)
	require.Equal(borges.LibraryID("foo"), lib.ID())

	require.NoError(lib.SetVersion(10))

	// check modified version
	lib, err = NewLibrary("", fs, &LibraryOptions{})
	require.NoError(err)

	version, err = lib.Version()
	require.NoError(err)
	require.Equal(10, version)
	require.Equal(borges.LibraryID("test"), lib.ID())
}

func TestMetadataLocationWrite(t *testing.T) {
	require := require.New(t)
	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

	lib, err := NewLibrary("test", fs, &LibraryOptions{})
	require.NoError(err)

	loc, err := lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
	require.NoError(err)

	l, ok := loc.(*Location)
	require.True(ok, "location must be siva.Location")

	var repos []string
	it, err := l.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	err = it.ForEach(func(r borges.Repository) error {
		repos = append(repos, r.ID().String())
		return nil
	})
	require.NoError(err)

	require.ElementsMatch([]string{
		"gitserver.com/a",
		"gitserver.com/b",
		"gitserver.com/c",
		"gitserver.com/d",
		"gitserver.com/e",
	}, repos)

	last := l.LastVersion()
	require.Equal(-1, last)

	l.SetVersion(0, &Version{
		Offset: 3180,
	})

	last = l.LastVersion()
	require.Equal(0, last)

	err = l.SaveMetadata()
	require.NoError(err)

	repos = []string{}
	it, err = l.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	err = it.ForEach(func(r borges.Repository) error {
		repos = append(repos, r.ID().String())
		return nil
	})
	require.NoError(err)

	require.ElementsMatch([]string{"gitserver.com/a"}, repos)

	l.SetVersion(1, &Version{
		Offset: 6557,
	})
	l.DeleteVersion(0)
	err = l.SaveMetadata()
	require.NoError(err)

	// Reopen library and check versions

	lib, err = NewLibrary("test", fs, &LibraryOptions{})
	require.NoError(err)

	loc, err = lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
	require.NoError(err)

	l, ok = loc.(*Location)
	require.True(ok, "location must be siva.Location")

	_, err = l.Version(0)
	require.True(errLocVersionNotExists.Is(err),
		"version 0 should not exist, it was deleted")

	v, err := l.Version(1)
	require.NoError(err, "version 1 should exist")
	require.Equal(uint64(6557), v.Offset)

	require.Equal(1, l.LastVersion())

	repos = []string{}
	it, err = l.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	err = it.ForEach(func(r borges.Repository) error {
		repos = append(repos, r.ID().String())
		return nil
	})
	require.NoError(err)

	require.ElementsMatch([]string{
		"gitserver.com/a",
		"gitserver.com/b",
	}, repos)
}

func TestMetadataVersionOnCommit(t *testing.T) {
	require := require.New(t)
	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

	lib, err := NewLibrary("test", fs, &LibraryOptions{

		Transactional: true,
	})
	require.NoError(err)

	tests := []struct {
		version int
		offset  uint64
		size    uint64
	}{
		{
			version: 0,
			offset:  28358,
			size:    28359,
		},
		{
			version: 1,
			offset:  30589,
			size:    2231,
		},
		{
			version: 2,
			offset:  32876,
			size:    2287,
		},
	}

	// create versions
	for _, t := range tests {
		loc, err := lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
		require.NoError(err)

		repo, err := loc.Get("gitserver.com/a", borges.RWMode)
		require.NoError(err)

		sivaRepo := repo.(*Repository)
		sivaRepo.VersionOnCommit(t.version)

		r := repo.R()
		name := fmt.Sprintf("tag%v", t.version)
		_, err = r.CreateTag(name, plumbing.ZeroHash, nil)
		require.NoError(err)

		err = repo.Commit()
		require.NoError(err)
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("version-%v", test.version), func(t *testing.T) {
			require.NoError(lib.SetVersion(test.version))
			loc, err := lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
			require.NoError(err)

			sivaLoc := loc.(*Location)
			version, err := sivaLoc.Version(test.version)
			require.NoError(err, "version must exist")

			require.Equal(test.offset, version.Offset)
			require.Equal(test.size, version.Size)

			repo, err := loc.Get("gitserver.com/a", borges.ReadOnlyMode)
			require.NoError(err)

			r := repo.R()
			_, err = r.Tag(fmt.Sprintf("tag%v", test.version))
			require.NoError(err)
			_, err = r.Tag(fmt.Sprintf("tag%v", test.version+1))
			require.Error(err)
		})
	}

}

func TestMetadataLibraryID(t *testing.T) {
	require := require.New(t)
	fs := memfs.New()

	// lib with no stored metadata in MetadataReadOnly mode
	// doesn't generate ids
	lib, err := NewLibrary("", fs, &LibraryOptions{
		MetadataReadOnly: true,
	})
	require.NoError(err)

	require.Equal(string(lib.ID()), "")
	files, err := fs.ReadDir("")
	require.NoError(err)
	require.Len(files, 0)

	// lib creating metadata with the given id
	lib, err = NewLibrary("test", fs, &LibraryOptions{})
	require.NoError(err)

	require.Equal(string(lib.ID()), "test")

	// lib with empty id using stored metadata will get the id from the
	// metadata
	lib, err = NewLibrary("", fs, &LibraryOptions{})

	require.Equal(string(lib.ID()), "test")

	// lib will generate an id in case of an empty
	// id is given if there's no previous metadata
	fs = memfs.New()
	lib, err = NewLibrary("", fs, &LibraryOptions{})

	require.NotEmpty(string(lib.ID()))
}
