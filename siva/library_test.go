package siva

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/test"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func TestTimeout(t *testing.T) {
	var req = require.New(t)

	lib := setupLibrary(t, "test", &LibraryOptions{
		Bucket:  2,
		Timeout: 1 * time.Nanosecond,
	})

	var err error
	lib.locReg, err = newLocationRegistry(0)
	req.NoError(err)

	_, err = lib.Locations()
	req.EqualError(err, context.DeadlineExceeded.Error())

	_, err = lib.Repositories(borges.ReadOnlyMode)
	req.EqualError(err, context.DeadlineExceeded.Error())

	_, _, _, err = lib.Has("baz")
	req.EqualError(err, context.DeadlineExceeded.Error())
}

func TestLibrary(t *testing.T) {
	s := new(test.LibrarySuite)
	s.LibrarySingle = func() borges.Library {
		return setupLibrary(t, "foo", &LibraryOptions{})
	}

	suite.Run(t, s)
}

func TestLibraryRepositoriesError(t *testing.T) {
	require := require.New(t)

	path, err := ioutil.TempDir("", "go-borges-siva")
	require.NoError(err)
	defer os.RemoveAll(path)

	fs := osfs.New(path)

	f, err := fs.Create("bad1.siva")
	require.NoError(err)
	_, err = f.Write([]byte("bad"))
	require.NoError(err)
	err = f.Close()
	require.NoError(err)

	f, err = fs.Create("bad2.siva")
	require.NoError(err)
	_, err = f.Write([]byte("bad"))
	require.NoError(err)
	err = f.Close()
	require.NoError(err)

	orig, err := os.Open("../_testdata/rooted/cf2e799463e1a00dbd1addd2003b0c7db31dbfe2.siva")
	require.NoError(err)

	f, err = fs.Create("good.siva")
	require.NoError(err)
	_, err = io.Copy(f, orig)
	require.NoError(err)
	err = f.Close()
	require.NoError(err)

	err = orig.Close()
	require.NoError(err)

	lib, err := NewLibrary("siva", fs, &LibraryOptions{
		RootedRepo: true,
	})
	require.NoError(err)

	it, err := lib.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var errors int
	var repos int
	var count int
	for {
		repo, err := it.Next()
		if err == io.EOF {
			break
		}

		if err == nil {
			repo.Close()
			repos++
		} else {
			errors++
		}

		count++

		if count > 7 {
			break
		}
	}

	require.Equal(7, count)
	require.Equal(2, errors)
	require.Equal(5, repos)
}
