package siva

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	borges "github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	fixtures "gopkg.in/src-d/go-git-fixtures.v3"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
)

func TestRepository(t *testing.T) {
	suite.Run(t, new(repoSuite))
}

func TestRepository_Transactional(t *testing.T) {
	suite.Run(t, &repoSuite{transactional: true})

}

type repoSuite struct {
	suite.Suite

	transactional bool
	lib           *Library
}

var (
	_ suite.SetupTestSuite    = (*repoSuite)(nil)
	_ suite.TearDownTestSuite = (*repoSuite)(nil)
)

func (s *repoSuite) SetupTest() {
	s.lib = setupLibrary(s.T(), "test", LibraryOptions{
		Transactional: s.transactional,
	})
}

func (s *repoSuite) TearDownTest() { s.lib = nil }

func (s *repoSuite) TestID() {
	var require = s.Require()

	expected := []borges.RepositoryID{"github.com/foo/qux", "github.com/foo/bar"}
	i, err := s.lib.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	var reposID []borges.RepositoryID
	require.NoError(i.ForEach(func(repo borges.Repository) error {
		r, ok := repo.(*Repository)
		require.True(ok)

		reposID = append(reposID, r.ID())
		return nil
	}))

	require.ElementsMatch(expected, reposID)
}

func (s *repoSuite) TestLocationID() {
	var require = s.Require()

	expected := map[borges.RepositoryID]borges.LocationID{
		"github.com/foo/qux": "foo-qux",
		"github.com/foo/bar": "foo-bar",
	}

	i, err := s.lib.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	require.NoError(i.ForEach(func(repo borges.Repository) error {
		r, ok := repo.(*Repository)
		require.True(ok)

		locID, ok := expected[r.ID()]
		require.True(ok)
		require.Equal(locID, r.LocationID())

		return nil
	}))
}

func (s *repoSuite) TestMode() {
	var require = s.Require()

	loc, err := s.lib.Location("foo-qux")
	require.NoError(err)

	// ReadOnlyMode on a single repository
	r, err := loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	require.Equal(borges.ReadOnlyMode, r.Mode())
	require.NoError(r.Close())

	// RWMode on a singlie repository
	r, err = loc.Get("github.com/foo/qux", borges.RWMode)
	require.NoError(err)

	require.Equal(borges.RWMode, r.Mode())
	require.NoError(r.Close())

	// ReadOnlyMode on all repositories
	i, err := s.lib.Repositories(borges.ReadOnlyMode)
	require.NoError(err)

	require.NoError(i.ForEach(func(repo borges.Repository) error {
		r, ok := repo.(*Repository)
		require.True(ok)

		require.Equal(borges.ReadOnlyMode, r.Mode())
		require.NoError(r.Close())
		return nil
	}))

	// RWMode on all repositories
	i, err = s.lib.Repositories(borges.RWMode)
	require.NoError(err)

	require.NoError(i.ForEach(func(repo borges.Repository) error {
		r, ok := repo.(*Repository)
		require.True(ok)

		require.Equal(borges.RWMode, r.Mode())
		require.NoError(r.Close())
		return nil
	}))
}
func (s *repoSuite) TestR() {
	var require = s.Require()

	i, err := s.lib.Repositories(borges.RWMode)
	require.NoError(err)

	require.NoError(i.ForEach(func(repo borges.Repository) error {
		_, ok := repo.(*Repository)
		require.True(ok)

		r := repo.R()
		require.NotNil(r)

		c, err := r.Config()
		require.NoError(err)
		require.Len(c.Remotes, 1)

		_, ok = c.Remotes["https://"+repo.ID().String()]
		require.True(ok)
		return nil
	}))
}

func (s *repoSuite) TestCommit_ReadOnly() {
	var require = s.Require()

	loc, err := s.lib.Location("foo-qux")
	require.NoError(err)

	r, err := loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	head, err := r.R().Head()
	require.NoError(err)

	_, err = r.R().CreateTag("new-tag", head.Hash(), nil)
	require.True(util.ErrReadOnlyStorer.Is(err))

	err = r.Commit()
	if s.transactional {
		require.NoError(err)
	} else {
		require.True(borges.ErrNonTransactional.Is(err))
	}
}

func (s *repoSuite) TestCommit_RW() {
	var require = s.Require()

	loc, err := s.lib.Location("foo-qux")
	require.NoError(err)

	r, err := loc.Get("github.com/foo/qux", borges.RWMode)
	require.NoError(err)

	head := createTagOnHead(s.T(), r, "new-tag")

	// Create test file in repository FS
	fs := r.FS()
	require.NotNil(fs)

	f, err := fs.Create("test-file")
	require.NoError(err)

	err = f.Close()
	require.NoError(err)

	var checker borges.Repository
	if s.transactional {
		// newly repositories opened before commit
		//  should see the previous state
		checker, err := loc.Get("github.com/foo/qux",
			borges.ReadOnlyMode)
		require.NoError(err)

		_, err = checker.R().Tag("new-tag")
		require.EqualError(err, git.ErrTagNotFound.Error())

		require.NoError(r.Commit())
		require.True(ErrRepoAlreadyClosed.Is(r.Commit()))
	} else {
		require.NoError(r.Close())
	}

	checker, err = loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	ref, err := checker.R().Tag("new-tag")
	require.NoError(err)
	require.Equal(head.Hash(), ref.Hash())

	// Check that test file is in repository's filesystem
	fs = checker.FS()
	require.NotNil(fs)

	_, err = fs.Stat("test-file")
	require.NoError(err)

	_, err = fs.Stat("config")
	require.NoError(err)
}

func (s *repoSuite) TestClose_ReadOnly() {
	var require = s.Require()

	loc, err := s.lib.Location("foo-qux")
	require.NoError(err)

	r, err := loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	require.NoError(r.Close())
	require.True(ErrRepoAlreadyClosed.Is(r.Close()))
}

func (s *repoSuite) TestClose_RW() {
	var require = s.Require()

	loc, err := s.lib.Location("foo-qux")
	require.NoError(err)

	r, err := loc.Get("github.com/foo/qux", borges.RWMode)
	require.NoError(err)

	head := createTagOnHead(s.T(), r, "new-tag")

	require.NoError(r.Close())

	r, err = loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	ref, err := r.R().Tag("new-tag")
	if s.transactional {
		require.EqualError(err, git.ErrTagNotFound.Error())
	} else {
		require.NoError(err)
		require.Equal(head.Hash(), ref.Hash())
	}
}

func (s *repoSuite) TestFilesystem() {
	var require = s.Require()

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	r, err := loc.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	repo := r.R()
	st, ok := repo.Storer.(*Storage)
	require.True(ok)

	_, err = repo.Remote("remote-test")
	require.EqualError(err, git.ErrRemoteNotFound.Error())

	expectedConf := config.NewConfig()
	expectedConf.Remotes["remote-test"] = &config.RemoteConfig{
		Fetch: []config.RefSpec{"+refs/heads/*:refs/remotes/remote-test/*"},
		Name:  "remote-test",
		URLs:  []string{"git@github.com:baz/bar.git"},
	}

	fs := st.filesystem()
	f, err := fs.OpenFile("config", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	require.NoError(err)

	buf, err := expectedConf.Marshal()
	require.NoError(err)

	_, err = f.Write(buf)
	require.NoError(err)

	err = f.Close()
	require.NoError(err)

	_, err = repo.Remote("remote-test")
	require.NoError(err)
}

func (s *repoSuite) TestPackfileWriter() {
	var require = s.Require()

	err := fixtures.Init()
	require.NoError(err)
	fixture := fixtures.Basic().One()
	defer fixtures.Clean()

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	r, err := loc.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	repo := r.R()
	remote, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{fixture.DotGit().Root()},
	})
	require.NoError(err)

	err = remote.Fetch(&git.FetchOptions{})
	require.NoError(err)

	sto, ok := repo.Storer.(*Storage)
	require.True(ok, "it's not a siva.Storage")
	fs := sto.filesystem()

	files, err := fs.ReadDir("/objects/pack")
	require.NoError(err)
	require.Len(files, 2)

	names := []string{files[0].Name(), files[1].Name()}
	expected := []string{
		"pack-ca7509c8118d8d2cc0977d9045311a8a627532d6.idx",
		"pack-ca7509c8118d8d2cc0977d9045311a8a627532d6.pack",
	}
	require.ElementsMatch(expected, names)
}

func (s *repoSuite) TestTransaction_Timeout() {
	if !s.transactional {
		s.T().SkipNow()
	}

	var require = s.Require()

	s.lib.options.Timeout = 100 * time.Millisecond

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	_, err = loc.Get("http://github.com/foo/bar", borges.ReadOnlyMode)
	require.True(borges.ErrRepositoryNotExists.Is(err))

	r, err := loc.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	_, err = loc.Init("http://github.com/foo/baz")
	require.Error(err)
	require.True(ErrTransactionTimeout.Is(err))
}

func (s *repoSuite) TestTransaction_FailedCommit() {
	if !s.transactional {
		s.T().SkipNow()
	}
	var require = s.Require()

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	r, err := loc.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)
	err = r.Commit()
	require.NoError(err)

	ls, ok := loc.(*Location)
	require.True(ok)
	stat, err := s.lib.fs.Stat(ls.path)
	require.NoError(err)

	origSize := stat.Size()

	// produce error in transaction
	rs, ok := r.(*Repository)
	require.True(ok)
	sto, ok := rs.s.(*Storage)
	require.True(ok)
	err = sto.cleanup()
	require.NoError(err)

	err = r.Commit()
	require.Error(err)

	stat, err = s.lib.fs.Stat(ls.path)
	require.NoError(err)
	require.Equal(origSize, stat.Size())
}

func (s *repoSuite) TestTransaction_FailedCommitEmpty() {
	if !s.transactional {
		s.T().SkipNow()
	}
	var require = s.Require()

	loc, err := s.lib.AddLocation("test")
	require.NoError(err)

	r, err := loc.Init("http://github.com/foo/bar")
	require.NoError(err)
	require.NotNil(r)

	// produce error in transaction
	rs, ok := r.(*Repository)
	require.True(ok)
	sto, ok := rs.s.(*Storage)
	require.True(ok)
	err = sto.cleanup()
	require.NoError(err)

	err = r.Commit()
	require.True(ErrEmptyCommit.Is(err))

	ls, ok := loc.(*Location)
	require.True(ok)

	_, err = s.lib.fs.Stat(ls.path)
	require.True(os.IsNotExist(err))
}

func TestTransactional_Concurrent_RW_Operations(t *testing.T) {
	// This test can't be performed using a memfs as billy.Filesytem
	// for the library because its storage is not thread safe. Trying
	// to make concurrent operations on the files hold by a memfs will
	// raise a panic.
	var require = require.New(t)

	fs, _ := setupOSFS(t, 0)

	lib, err := NewLibrary("test", fs, LibraryOptions{Transactional: true})
	require.NoError(err)

	loc, err := lib.Location("foo-qux")
	require.NoError(err)

	const (
		tag          = "new-tag"
		transactions = 1000
	)

	var (
		w     sync.WaitGroup
		count int
	)

	for i := 0; i < transactions; i++ {
		w.Add(1)
		go func(id int) {
			defer w.Done()

			r, err := loc.Get("github.com/foo/qux", borges.RWMode)
			require.NoError(err)

			name := fmt.Sprintf("%s-%d", tag, id)
			createTagOnHead(t, r, name)
			require.NoError(r.Commit())
		}(count)
		count++
	}
	w.Wait()

	r, err := loc.Get("github.com/foo/qux", borges.ReadOnlyMode)
	require.NoError(err)

	head, err := r.R().Head()
	require.NoError(err)

	for i := 0; i < transactions; i++ {
		name := fmt.Sprintf("%s-%d", tag, i)
		ref, err := r.R().Tag(name)
		require.NoError(err)
		require.Equal(head.Hash(), ref.Hash())
	}

	require.NoError(r.Close())
}
