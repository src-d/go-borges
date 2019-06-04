package siva

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/ghodss/yaml"
	"github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func TestMetadataSiva(t *testing.T) {
	require := require.New(t)

	meta := LocationMetadata{
		Versions: map[int]Version{
			0: {
				Offset: 16,
				Size:   17,
			},
			1: {
				Offset: 32,
				Size:   33,
			},
			10: {
				Offset: 42,
				Size:   43,
			},
			20: {
				Offset: 52,
				Size:   53,
			},
			2: {
				Offset: 62,
				Size:   63,
			},
		},
	}

	data, err := yaml.Marshal(meta)
	require.NoError(err)

	m, err := parseLocationMetadata(data)
	require.NoError(err)
	require.EqualValues(meta, *m)
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

		libMetadata = `---
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
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("version-%v", test.version), func(t *testing.T) {
			require := require.New(t)
			fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

			if test.version != -1 {
				libMd := libMetadata + strconv.Itoa(test.version)
				err := util.WriteFile(fs, LibraryMetadataFile, []byte(libMd), 0666)
				require.NoError(err)
			}

			path := locationMetadataPath("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2.siva")
			err := util.WriteFile(fs, path, []byte(rootedVersions), 0666)
			require.NoError(err)

			lib, err := NewLibrary("test", fs, LibraryOptions{})
			require.NoError(err)

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

func TestMetadataWriteLibrary(t *testing.T) {
	require := require.New(t)
	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

	// library does not have metadata
	lib, err := NewLibrary("test", fs, LibraryOptions{})
	require.NoError(err)

	version := lib.Version()
	require.Equal(-1, version)

	err = lib.SaveMetadata()
	require.NoError(err)

	_, err = fs.Stat(LibraryMetadataFile)
	require.True(os.IsNotExist(err), "library metadata file should not exist")

	// set version in library metadata
	lib, err = NewLibrary("test", fs, LibraryOptions{})
	require.NoError(err)

	version = lib.Version()
	require.Equal(-1, version)

	lib.SetVersion(1)

	err = lib.SaveMetadata()
	require.NoError(err)

	_, err = fs.Stat(LibraryMetadataFile)
	require.NoError(err, "library metadata file should exist")

	// modify version in library metadata
	lib, err = NewLibrary("test", fs, LibraryOptions{})
	require.NoError(err)

	version = lib.Version()
	require.Equal(1, version)

	lib.SetVersion(10)
	err = lib.SaveMetadata()
	require.NoError(err)

	// check modified version
	lib, err = NewLibrary("test", fs, LibraryOptions{})
	require.NoError(err)

	version = lib.Version()
	require.Equal(10, version)
}

func TestMetadataWriteLocation(t *testing.T) {
	require := require.New(t)
	fs, _ := setupFS(t, "../_testdata/rooted", true, 0)

	lib, err := NewLibrary("test", fs, LibraryOptions{})
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
	}, repos)

	last := l.LastVersion()
	require.Equal(-1, last)

	l.SetVersion(0, Version{
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

	l.SetVersion(1, Version{
		Offset: 6557,
	})
	l.DeleteVersion(0)
	err = l.SaveMetadata()
	require.NoError(err)

	// Reopen library and check versions

	lib, err = NewLibrary("test", fs, LibraryOptions{})
	require.NoError(err)

	loc, err = lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
	require.NoError(err)

	l, ok = loc.(*Location)
	require.True(ok, "location must be siva.Location")

	_, ok = l.Version(0)
	require.False(ok, "version 0 should not exist, it was deleted")

	v, ok := l.Version(1)
	require.True(ok, "version 1 should exist")
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

	lib, err := NewLibrary("test", fs, LibraryOptions{
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
			offset:  21250,
			size:    21251,
		},
		{
			version: 1,
			offset:  23323,
			size:    2073,
		},
		{
			version: 2,
			offset:  25452,
			size:    2129,
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
			lib.SetVersion(test.version)
			loc, err := lib.Location("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2")
			require.NoError(err)

			sivaLoc := loc.(*Location)
			version, ok := sivaLoc.Version(test.version)
			require.True(ok, "version must exist")

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
