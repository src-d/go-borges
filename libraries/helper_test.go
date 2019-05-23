package libraries

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

const (
	testLib1 = "../_testdata/lib1"
	testLib2 = "../_testdata/lib2"
	testLib3 = "../_testdata/lib3"
)

var (
	testLibs = map[borges.LibraryID]map[borges.LocationID][]borges.RepositoryID{
		"lib1": map[borges.LocationID][]borges.RepositoryID{
			"1880dc904e1b2774be9c97a7b85efabdb910f974": []borges.RepositoryID{
				"github.com/jtleek/datasharing",
				"github.com/diptadhi/datasharing",
				"github.com/nmorr041/datasharing",
			},
			"6671f3b1147324f4fb1fbbe2aba843031738f59e": []borges.RepositoryID{
				"github.com/enaqx/awesome-pentest",
				"github.com/Inter1292/awesome-pentest",
				"github.com/apelsin83/awesome-pentest",
			},
		},
		"lib2": map[borges.LocationID][]borges.RepositoryID{
			"cce60e1b6fb7ad56d07cbcaee7a62030f7d01777": []borges.RepositoryID{
				"github.com/kahun/awesome-sysadmin",
				"github.com/apoliukh/awesome-sysadmin",
				"github.com/gauravaristocrat/awesome-sysadmin",
			},
			"fe83b066a45d859cd40cbf512c4ec20351c4f9d9": []borges.RepositoryID{
				"github.com/MunGell/awesome-for-beginners",
				"github.com/dhruvil1514/awesome-for-beginners",
				"github.com/karellism/awesome-for-beginners",
			},
			"3974996807a9f596cf25ac3a714995c24bb97e2c": []borges.RepositoryID{
				"github.com/rtyley/small-test-repo",
				"github.com/kuldeep992/small-test-repo",
				"github.com/kuldeep-singh-blueoptima/small-test-repo",
			},
		},
		"lib3": map[borges.LocationID][]borges.RepositoryID{
			"a6c64c655d15afda789f8138b83213782b6f77c7": []borges.RepositoryID{
				"github.com/prakhar1989/awesome-courses",
				"github.com/Leo-xxx/awesome-courses",
				"github.com/manjunath00/awesome-courses",
			},
			"f2cee90acf3c6644d51a37057845b98ab1580932": []borges.RepositoryID{
				"github.com/jtoy/awesome-tensorflow",
				"github.com/SiweiLuo/awesome-tensorflow",
				"github.com/youtang1993/awesome-tensorflow",
			},
		},
	}
)

func setupSivaLibraries(t *testing.T, opts siva.LibraryOptions) *Libraries {
	t.Helper()
	var require = require.New(t)

	libs := New(Options{})
	require.NotNil(libs)
	require.Equal(borges.LibraryID(""), libs.ID())

	sivaLibs := []string{testLib1, testLib2, testLib3}
	for _, l := range sivaLibs {
		lib := setupSivaLibrary(t, l, opts)
		require.NoError(libs.Add(lib))

		_, err := libs.Library(lib.ID())
		require.NoError(err)
	}

	return libs
}

func setupSivaLibrary(t *testing.T, path string, opts siva.LibraryOptions) *siva.Library {
	t.Helper()
	var require = require.New(t)

	fs := buildTestFS(t, path)
	lib, err := siva.NewLibrary(filepath.Base(path), fs, opts)
	require.NoError(err)

	return lib
}

func buildTestFS(t *testing.T, testDir string) billy.Filesystem {
	t.Helper()

	source := osfs.New(testDir)
	dest := memfs.New()
	copyToMem(t, source, dest, "")

	return dest
}

func copyToMem(t *testing.T, source, dest billy.Filesystem, path string) {
	t.Helper()
	var require = require.New(t)

	entries, err := source.ReadDir(path)
	require.NoError(err)

	for _, e := range entries {
		if !(e.IsDir() || e.Mode().IsRegular()) {
			continue
		}

		abs := filepath.Join(path, e.Name())
		if e.IsDir() {
			copyToMem(t, source, dest, abs)
		} else {
			r, err := source.Open(abs)
			require.NoError(err)

			w, err := dest.Create(abs)
			require.NoError(err)

			_, err = io.Copy(w, r)
			require.NoError(err)
		}
	}
}
