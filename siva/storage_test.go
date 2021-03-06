package siva

import (
	"bufio"
	"strings"
	"testing"

	billy "gopkg.in/src-d/go-billy.v4"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem/dotgit"
	"gopkg.in/src-d/go-git.v4/storage/transactional"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

func TestStorage(t *testing.T) {
	suite.Run(t, new(storageSuite))
}

func TestStorage_Transactional(t *testing.T) {
	suite.Run(t, &storageSuite{transactional: true})

}

type storageSuite struct {
	suite.Suite

	transactional bool
	lib           *Library
}

var (
	_ suite.SetupTestSuite    = (*storageSuite)(nil)
	_ suite.TearDownTestSuite = (*storageSuite)(nil)
)

func (s *storageSuite) SetupTest() {
	fs, _ := setupFS(s.T(), testRootedDir, true, 0)
	lib, err := NewLibrary("not-rooted", fs, &LibraryOptions{
		RootedRepo:    false,
		Transactional: true,
	})
	require.NoError(s.T(), err)

	s.lib = lib
}

func (s *storageSuite) TearDownTest() { s.lib = nil }

func (s *storageSuite) TestCleanupTmp() {
	var req = require.New(s.T())
	req.True(true)

	fs := s.lib.tmp

	entries, err := fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) == 0)

	// check tmp files are removed on commit

	r, err := s.lib.Get("gitserver.com/a", borges.RWMode)
	req.NoError(err)

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) != 0)

	req.True(ErrEmptyCommit.Is(r.Commit()))

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) == 0)

	// check tmp files are removed on close

	r, err = s.lib.Get("gitserver.com/a", borges.RWMode)
	req.NoError(err)

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) != 0)

	req.NoError(r.Close())

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) == 0)

	// check tmp files are removed on failed commit

	r, err = s.lib.Get("gitserver.com/a", borges.RWMode)
	req.NoError(err)

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) != 0)

	sto, ok := r.R().Storer.(*Storage)
	req.True(ok)

	sto.Storer = &fakeStorer{sto.Storer}
	r.R().Storer = sto
	req.EqualError(r.Commit(), errFake.New().Error())

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) == 0)

	// check tmp files are removed on failed close

	r, err = s.lib.Get("gitserver.com/a", borges.RWMode)
	req.NoError(err)

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) != 0)

	sto, ok = r.R().Storer.(*Storage)
	req.True(ok)

	sto.Storer = &fakeStorer{sto.Storer}
	r.R().Storer = sto
	req.EqualError(r.Close(), errFake.New().Error())

	entries, err = fs.ReadDir("/")
	req.NoError(err)
	req.True(len(entries) == 0)
}

type fakeStorer struct {
	storage.Storer
}

var errFake = errors.NewKind("fake error")

func (s *fakeStorer) Close() error  { return errFake.New() }
func (s *fakeStorer) Commit() error { return errFake.New() }

func (s *storageSuite) TestReference_Storage() {
	var require = require.New(s.T())

	r, err := s.lib.Get("gitserver.com/a", borges.RWMode)
	require.NoError(err)

	iter, err := r.R().References()
	require.NoError(err)

	var currentRefs []*plumbing.Reference
	require.NoError(iter.ForEach(func(ref *plumbing.Reference) error {
		if !(ref.Name() == plumbing.HEAD &&
			ref.Type() == plumbing.SymbolicReference) {
			currentRefs = append(currentRefs, ref)
		}

		return nil
	}))

	require.True(len(currentRefs) > 0)

	sto, ok := r.R().Storer.(*Storage)
	require.True(ok)

	newRefs := []*plumbing.Reference{
		plumbing.NewHashReference(
			plumbing.NewBranchReferenceName("test"),
			plumbing.ZeroHash,
		),
		plumbing.NewHashReference(
			plumbing.NewTagReferenceName("v0.0.0-test.1"),
			plumbing.ZeroHash,
		),
		plumbing.NewHashReference(
			plumbing.NewRemoteReferenceName("origin", "foo"),
			plumbing.ZeroHash,
		),
	}

	for _, ref := range newRefs {
		require.NoError(sto.SetReference(ref))
	}

	require.NoError(sto.Commit())

	expected := append(currentRefs, newRefs...)
	require.ElementsMatch(expected, readPackedRefs(s.T(), sto))

	require.NoError(sto.RemoveReference(newRefs[len(newRefs)-1].Name()))
	require.NoError(sto.Commit())

	expected = expected[:len(expected)-1]
	require.ElementsMatch(expected, readPackedRefs(s.T(), sto))

	if s.lib.options.Transactional {
		require.NoError(r.Commit())
	}

	r, err = s.lib.Get("gitserver.com/a", borges.RWMode)
	require.NoError(err)

	sto, ok = r.R().Storer.(*Storage)
	require.True(ok)

	_, err = sto.baseFS.Stat(packedRefsPath)
	require.NoError(err)

	entries, err := sto.baseFS.ReadDir(refsPath)
	require.NoError(err)

	require.True(len(entries) == 1)
	require.True(entries[0].Name() == keepFile)
}

func readPackedRefs(t *testing.T, sto *Storage) []*plumbing.Reference {
	t.Helper()
	var require = require.New(t)

	var fs billy.Filesystem
	switch st := sto.Storer.(type) {
	case *filesystem.Storage:
		fs = st.Filesystem()
	case transactional.Storage:
		transFs, err := getSivaFS(sto.base, sto.path, sto.tmp, "test")
		require.NoError(err)

		fs = transFs
	}

	f, err := fs.Open(packedRefsPath)
	require.NoError(err)

	refs, err := findPackedRefsInFile(f)
	require.NoError(err)

	return refs
}

// findPackedRefsInFile adapted from https://github.com/src-d/go-git/blob/923642abf033cd40b5f3aa5205e517d1feb32f4d/storage/filesystem/dotgit/dotgit.go#L653
func findPackedRefsInFile(f billy.File) ([]*plumbing.Reference, error) {
	s := bufio.NewScanner(f)
	var refs []*plumbing.Reference
	for s.Scan() {
		ref, err := processLine(s.Text())
		if err != nil {
			return nil, err
		}

		if ref != nil {
			refs = append(refs, ref)
		}
	}

	return refs, s.Err()
}

// proccessLine adapted from https://github.com/src-d/go-git/blob/923642abf033cd40b5f3aa5205e517d1feb32f4d/storage/filesystem/dotgit/dotgit.go#L852
func processLine(line string) (*plumbing.Reference, error) {
	if len(line) == 0 {
		return nil, nil
	}

	switch line[0] {
	case '#': // comment - ignore
		return nil, nil
	case '^': // annotated tag commit of the previous line - ignore
		return nil, nil
	default:
		ws := strings.Split(line, " ") // hash then ref
		if len(ws) != 2 {
			return nil, dotgit.ErrPackedRefsBadFormat
		}

		return plumbing.NewReferenceFromStrings(ws[1], ws[0]), nil
	}
}

func (s *storageSuite) TestCloseSivaFilesForReadOnlyStorage() {
	var req = require.New(s.T())

	iter, err := s.lib.Repositories(borges.ReadOnlyMode)
	req.NoError(err)

	req.NoError(iter.ForEach(func(r borges.Repository) error {
		s.T().Run(string(r.ID()), func(t *testing.T) {
			repo, ok := r.(*Repository)
			req.True(ok)

			sto, ok := repo.s.(*ReadOnlyStorer)
			req.True(ok)

			testSto := &testReadOnlyStorer{ReadOnlyStorer: sto}
			repo.s = testSto
			req.NoError(repo.Close())
			req.True(testSto.closed)
		})
		return nil
	}))
}

type testReadOnlyStorer struct {
	*ReadOnlyStorer
	closed bool
}

func (s *testReadOnlyStorer) Close() error {
	if err := s.ReadOnlyStorer.Close(); err != nil {
		return err
	}

	s.closed = true
	return nil
}
