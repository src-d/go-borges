package siva

import (
	"bufio"
	"strings"
	"testing"

	billy "gopkg.in/src-d/go-billy.v4"
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
	s.lib = setupLibrary(s.T(), "test", LibraryOptions{
		Transactional: s.transactional,
	})
}

func (s *storageSuite) TearDownTest() { s.lib = nil }

func (s *storageSuite) TestReference_Storage() {
	var require = s.Require()

	r, err := s.lib.Get("github.com/foo/bar", borges.RWMode)
	require.NoError(err)

	sto, ok := r.R().Storer.(*Storage)
	require.True(ok)

	master, err := sto.Reference("refs/heads/master")
	require.NoError(err)
	require.Equal(
		plumbing.NewHash("cd908f91d6721f6f91db00d0cac3b5a12b322354"),
		master.Hash(),
	)

	newRefs := []*plumbing.Reference{
		plumbing.NewHashReference(
			plumbing.NewBranchReferenceName("test"),
			plumbing.NewHash("cd908f91d6721f6f91db00d0cac3b5a12b322354"),
		),
		plumbing.NewHashReference(
			plumbing.NewTagReferenceName("v0.0.0-test.1"),
			plumbing.NewHash("cd908f91d6721f6f91db00d0cac3b5a12b322354"),
		),
		plumbing.NewHashReference(
			plumbing.NewRemoteReferenceName("origin", "master"),
			plumbing.NewHash("cd908f91d6721f6f91db00d0cac3b5a12b322354"),
		),
	}

	for _, ref := range newRefs {
		require.NoError(sto.SetReference(ref))
	}

	require.NoError(sto.Commit())

	expected := []*plumbing.Reference{master}
	expected = append(expected, newRefs...)
	require.ElementsMatch(expected, s.readPackedRefs(sto))

	require.NoError(sto.RemoveReference(newRefs[len(newRefs)-1].Name()))
	require.NoError(sto.Commit())

	expected = expected[:len(expected)-1]
	require.ElementsMatch(expected, s.readPackedRefs(sto))
}

func (s *storageSuite) readPackedRefs(sto *Storage) []*plumbing.Reference {
	s.T().Helper()
	var require = require.New(s.T())

	var fs billy.Filesystem
	switch st := sto.Storer.(type) {
	case *filesystem.Storage:
		fs = st.Filesystem()
	case transactional.Storage:
		transFs, err := getSivaFS(sto.base, "foo-bar.siva", s.lib.tmp, "test")
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
