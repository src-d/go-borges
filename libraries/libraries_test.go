package libraries

import (
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"

	"os"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestLibraries(t *testing.T) {
	suite.Run(t, &librariesSuite{bucket: 2, transactional: false})
	suite.Run(t, &librariesSuite{bucket: 2, transactional: true})
}

type librariesSuite struct {
	suite.Suite

	bucket        int
	transactional bool
	libs          *Libraries
}

func (s *librariesSuite) SetupSuite() {
	s.libs = setupSivaLibraries(s.T(), &siva.LibraryOptions{
		Bucket:        s.bucket,
		Transactional: s.transactional,
	})
}

func (s *librariesSuite) TestNotImplemented() {
	var require = s.Require()

	_, err := s.libs.Init(context.TODO(), "foo")
	require.True(borges.ErrNotImplemented.Is(err))

	_, err = s.libs.GetOrInit(context.TODO(), "foo")
	require.True(borges.ErrNotImplemented.Is(err))
}

func (s *librariesSuite) TestLibraryAndLocationAndHasAndGet() {
	var require = s.Require()

	for lib, locations := range testLibs {
		_, err := s.libs.Library(context.TODO(), lib)
		require.NoError(err)
		for loc, repos := range locations {
			_, err := s.libs.Location(context.TODO(), loc)
			require.NoError(err)
			for _, repo := range repos {
				ok, libID, locID, err := s.libs.Has(context.TODO(), repo)
				require.NoError(err)
				require.True(ok, repo.String())
				require.Equal(lib, libID)
				require.Equal(loc, locID)

				_, err = s.libs.Get(context.TODO(), repo, borges.ReadOnlyMode)
				require.NoError(err)
			}
		}
	}
}

func (s *librariesSuite) TestRepositories() {
	var require = s.Require()

	var expected []borges.RepositoryID
	for _, locations := range testLibs {
		for _, repos := range locations {
			for _, repo := range repos {
				expected = append(expected, repo)
			}
		}
	}

	iter, err := s.libs.Repositories(context.TODO(), borges.ReadOnlyMode)
	require.NoError(err)

	var ids []borges.RepositoryID
	require.NoError(iter.ForEach(func(r borges.Repository) error {
		ids = append(ids, r.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestLocations() {
	var require = s.Require()

	var expected []borges.LocationID
	for _, locations := range testLibs {
		for loc := range locations {
			expected = append(expected, loc)
		}
	}

	iter, err := s.libs.Locations(context.TODO())
	require.NoError(err)

	var ids []borges.LocationID
	require.NoError(iter.ForEach(func(l borges.Location) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestLibraries() {
	var require = s.Require()

	var expected []borges.LibraryID
	for lib := range testLibs {
		expected = append(expected, lib)
	}

	iter, err := s.libs.Libraries(context.TODO())
	require.NoError(err)

	var ids []borges.LibraryID
	require.NoError(iter.ForEach(func(l borges.Library) error {
		ids = append(ids, l.ID())
		return nil
	}))

	require.ElementsMatch(expected, ids)
}

func (s *librariesSuite) TestFilteredLibraries() {
	var require = s.Require()

	var filter FilterLibraryFunc = func(lib borges.Library) (bool, error) {
		_, ok := lib.(*plain.Library)
		return ok, nil
	}

	iter, err := s.libs.FilteredLibraries(filter)
	require.NoError(err)

	_, err = iter.Next()
	require.EqualError(err, io.EOF.Error())

	filter = func(lib borges.Library) (bool, error) {
		ok, _, _, err := lib.Has(context.TODO(), borges.RepositoryID("github.com/rtyley/small-test-repo"))
		return ok, err
	}

	iter, err = s.libs.FilteredLibraries(filter)
	require.NoError(err)

	lib, err := iter.Next()
	require.NoError(err)
	require.Equal(borges.LibraryID("lib2"), lib.ID())

	_, err = iter.Next()
	require.EqualError(err, io.EOF.Error())
}

func TestLibrariesRepositoriesError(t *testing.T) {
	require := require.New(t)

	// prepare plain library

	idqux := borges.LocationID("qux")
	lqux, _ := plain.NewLocation(idqux, osfs.New("/does/not/exist/qux"), nil)
	idbar := borges.LocationID("bar")
	lbar, _ := plain.NewLocation(idbar, osfs.New("/does/not/exist/bar"), nil)
	idbaz := borges.LocationID("baz")
	lbaz, _ := plain.NewLocation(idbaz, memfs.New(), nil)

	nbaz := borges.RepositoryID("github.com/source/bar")
	_, err := lbaz.Init(context.TODO(), nbaz)
	require.NoError(err)

	plainLib := plain.NewLibrary(borges.LibraryID("broken"))
	plainLib.AddLocation(lqux)
	plainLib.AddLocation(lbar)
	plainLib.AddLocation(lbaz)

	// prepare siva library

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

	sivaLib, err := siva.NewLibrary("siva", fs, &siva.LibraryOptions{
		RootedRepo: true,
	})
	require.NoError(err)

	lib := New(&Options{})
	lib.Add(plainLib)
	lib.Add(sivaLib)

	it, err := lib.Repositories(context.TODO(), borges.ReadOnlyMode)
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

		if count > 10 {
			break
		}
	}

	require.Equal(10, count)
	require.Equal(4, errors)
	require.Equal(6, repos)
}
