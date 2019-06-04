package siva

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-billy.v4/util"
)

func TestCheckpoint_Broken_Siva_File_No_Checkpoint(t *testing.T) {
	// this test should be integrated in the checkpointSuite which uses
	// memfs when the PR (https://github.com/src-d/go-billy/pull/68)
	// gets merged.

	var require = require.New(t)

	sivaData, err := ioutil.ReadFile("../_testdata/siva/foo-bar.siva")
	require.NoError(err)
	brokenData := append(sivaData[:], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)

	path, err := ioutil.TempDir("", "go-borges-siva")
	require.NoError(err)

	fs := osfs.New(path)

	err = util.WriteFile(fs, "really_broken.siva", brokenData, 0666)
	require.NoError(err)

	lib, err := NewLibrary("test", fs, LibraryOptions{Transactional: true})
	require.NoError(err)

	loc, err := lib.Location("really_broken")
	require.NoError(err)
	_, err = loc.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.Error(err)
}

func TestCheckpoint(t *testing.T) {
	suite.Run(t, new(checkpointSuite))
}

type checkpointSuite struct {
	suite.Suite

	fs    billy.Filesystem
	sivas []string
}

var _ suite.SetupTestSuite = (*checkpointSuite)(nil)
var _ suite.TearDownTestSuite = (*checkpointSuite)(nil)

func (s *checkpointSuite) SetupTest() {
	s.fs, s.sivas = setupMemFS(s.T(), 0)
}

func (s *checkpointSuite) TearDownTest() {
	s.fs = nil
	s.sivas = nil
}

func (s *checkpointSuite) TestNew() {
	for _, siva := range s.sivas {
		s.T().Run(siva, func(t *testing.T) {
			var require = s.Require()

			infoBefore, err := s.fs.Lstat(siva)
			require.NoError(err)

			cpPath := siva + checkpointExtension
			_, err = s.fs.Lstat(cpPath)
			require.EqualError(err, os.ErrNotExist.Error())

			cp, err := newCheckpoint(s.fs, siva, false)
			require.NoError(err)
			require.NotNil(cp)
			require.Equal(s.fs, cp.baseFs)
			require.Equal(siva, cp.path)
			require.Equal(cpPath, cp.persist)
			require.Equal(int64(-1), cp.offset)

			infoAfter, err := s.fs.Lstat(siva)
			require.NoError(err)

			require.Equal(infoBefore.Size(), infoAfter.Size())
		})
	}
}

func (s *checkpointSuite) TestNew_Prev_Checkpoint_File() {
	for _, siva := range s.sivas {
		s.T().Run(siva, func(t *testing.T) {
			var require = s.Require()

			cpPath := siva + checkpointExtension
			require.NoError(writeInt64(s.fs, cpPath, int64(10)))
			_, err := s.fs.Lstat(cpPath)
			require.NoError(err)

			cp, err := newCheckpoint(s.fs, siva, false)
			require.NoError(err)
			require.Equal(int64(10), cp.offset)

			err = cp.Apply()
			require.NoError(err)
			require.Equal(int64(-1), cp.offset)

			info, err := s.fs.Lstat(siva)
			require.NoError(err)
			require.Equal(int64(10), info.Size())
		})
	}
}

func (s *checkpointSuite) TestNew_Dangling_Checkpoint_File() {
	var require = s.Require()

	siva := "fake.siva"
	cpPath := siva + checkpointExtension
	require.NoError(writeInt64(s.fs, cpPath, int64(10)))
	_, err := s.fs.Lstat(cpPath)
	require.NoError(err)

	_, err = newCheckpoint(s.fs, siva, false)
	expected := ErrCannotUseSivaFile.Wrap(
		borges.ErrLocationNotExists.New(siva), siva).Error()
	require.EqualError(err, expected)

	_, err = s.fs.Lstat(cpPath)
	require.Error(err, os.ErrNotExist)
}

func (s *checkpointSuite) TestNew_Create() {
	var require = s.Require()

	siva := "fake.siva"
	_, err := newCheckpoint(s.fs, siva, false)
	expected := ErrCannotUseSivaFile.Wrap(
		borges.ErrLocationNotExists.New(siva), siva).Error()
	require.EqualError(err, expected)

	cp, err := newCheckpoint(s.fs, siva, true)
	require.NoError(err)
	require.NotNil(cp)
	require.Equal(s.fs, cp.baseFs)
	require.Equal(siva, cp.path)
	require.Equal(siva+checkpointExtension, cp.persist)
	require.Equal(int64(-1), cp.offset)
}

func (s *checkpointSuite) TestApply() {
	for _, siva := range s.sivas {
		s.T().Run(siva, func(t *testing.T) {
			var require = s.Require()

			cp, err := newCheckpoint(s.fs, siva, false)
			require.NoError(err)
			require.NotNil(cp)

			cp.offset = int64(10)
			require.NoError(cp.Apply())

			info, err := s.fs.Lstat(siva)
			require.NoError(err)

			require.Equal(int64(10), info.Size())
			require.Equal(int64(-1), cp.offset)
		})
	}
}

func (s *checkpointSuite) TestSave() {
	for _, siva := range s.sivas {
		s.T().Run(siva, func(t *testing.T) {
			var require = s.Require()

			info, err := s.fs.Lstat(siva)
			require.NoError(err)

			cpPath := siva + checkpointExtension
			_, err = s.fs.Lstat(cpPath)
			require.EqualError(err, os.ErrNotExist.Error())

			cp, err := newCheckpoint(s.fs, siva, false)
			require.NoError(err)
			require.NotNil(cp)

			require.NoError(cp.Save())

			num, err := readInt64(s.fs, cpPath)
			require.NoError(err)

			require.Equal(info.Size(), num)
		})
	}
}

func (s *checkpointSuite) TestReset() {
	for _, siva := range s.sivas {
		s.T().Run(siva, func(t *testing.T) {
			var require = s.Require()

			cp, err := newCheckpoint(s.fs, siva, false)
			require.NoError(err)
			require.NotNil(cp)

			require.NoError(cp.Save())

			info, err := s.fs.Lstat(siva)
			require.NoError(err)
			require.Equal(info.Size(), cp.offset)

			cpPath := siva + checkpointExtension
			_, err = s.fs.Lstat(cpPath)
			require.NoError(err)

			require.NoError(cp.Reset())
			require.Equal(int64(-1), cp.offset)
			_, err = s.fs.Lstat(cpPath)
			require.EqualError(err, os.ErrNotExist.Error())
		})
	}
}
