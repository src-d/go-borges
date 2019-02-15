package siva

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	borges "github.com/src-d/go-borges"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/util"
)

func TestCheckpoint(t *testing.T) {
	require := require.New(t)

	sivaData, err := ioutil.ReadFile("../_testdata/siva/foo-bar.siva")
	require.NoError(err)

	fs := memfs.New()
	lib, err := NewLibrary("test", fs, true)
	require.NoError(err)

	var l borges.Location

	// correct file

	err = util.WriteFile(fs, "correct.siva", sivaData, 0666)
	require.NoError(err)
	l, err = lib.Location("correct")
	require.NoError(err)
	_, err = l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)

	// broken file with correct checkpoint file

	size := strconv.Itoa(len(sivaData))
	err = util.WriteFile(fs, "broken.siva.checkpoint", []byte(size), 0666)
	require.NoError(err)
	brokenData := append(sivaData[:], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
	err = util.WriteFile(fs, "broken.siva", brokenData, 0666)
	require.NoError(err)
	l, err = lib.Location("broken")
	require.NoError(err)
	_, err = l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	require.NoError(err)
	_, err = fs.Stat("broken.siva.checkpoint")
	require.True(os.IsNotExist(err))

	// dangling checkpoint file

	size = strconv.Itoa(len(sivaData))
	err = util.WriteFile(fs, "dangling.siva.checkpoint", []byte(size), 0666)
	require.NoError(err)
	l, err = lib.Location("dangling")
	require.True(borges.ErrLocationNotExists.Is(err))
	_, err = fs.Stat("dangling.siva.checkpoint")
	require.True(os.IsNotExist(err))

	// broken siva file without checkpoint

	// TODO: there's a bug in memfs and it crashes with this test. This will be
	// enabled after it is fixed (check negative offsets in ReadAt/WriteAt).

	// err = util.WriteFile(fs, "really_broken.siva", brokenData, 0666)
	// require.NoError(err)
	// l, err = lib.Location("really_broken")
	// require.NoError(err)
	// _, err = l.Get("github.com/foo/bar", borges.ReadOnlyMode)
	// require.True(borges.ErrLocationNotExists.Is(err))
}
