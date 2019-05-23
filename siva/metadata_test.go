package siva

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/src-d/go-borges"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/util"
)

func TestMetadataSiva(t *testing.T) {
	require := require.New(t)

	meta := LocationMetadata{
		Versions: Versions{
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

	data, err := meta.ToYaml()
	require.NoError(err)

	m, err := ParseLocationMetadata(data)
	require.NoError(err)
	require.EqualValues(meta, *m)
}

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

func TestMetadataLibrary(t *testing.T) {

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

			path := LocationMetadataPath("cf2e799463e1a00dbd1addd2003b0c7db31dbfe2.siva")
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
