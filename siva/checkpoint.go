package siva

import (
	"os"
	"strconv"

	borges "github.com/src-d/go-borges"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	errors "gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrCannotUseCheckpointFile is returned on checkpoint problems.
	ErrCannotUseCheckpointFile = errors.NewKind("cannot use checkpoint file: %s")
	// ErrCannotUseSivaFile is returned on siva problems.
	ErrCannotUseSivaFile = errors.NewKind("cannot use siva file: %s")
)

const checkpointExtension = ".checkpoint"

// checkpoint tracks the status of a siva file and creates checkpoints to be
// able to return back to a known state of that siva file.
type checkpoint struct {
	offset  int64
	baseFs  billy.Filesystem
	path    string
	persist string
}

// newCheckpoint builds a new Checkpoint.
func newCheckpoint(fs billy.Filesystem, path string) (*checkpoint, error) {
	persist := path + checkpointExtension

	if _, err := fs.Stat(path); err != nil && os.IsNotExist(err) {
		cleanup(fs, persist)
		return nil, ErrCannotUseSivaFile.Wrap(
			borges.ErrLocationNotExists.New(path))
	}

	c := &checkpoint{
		baseFs:  fs,
		path:    path,
		persist: persist,
	}

	offset, err := readInt64(fs, persist)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, ErrCannotUseCheckpointFile.Wrap(err)
		}

		offset = 0
	}

	c.offset = offset

	if err := c.Apply(); err != nil {
		return nil, err
	}

	return c, nil
}

// Apply applies if necessary the operations on the siva file to
// leave it in the last correct state the checkpoint keeps.
func (c *checkpoint) Apply() error {
	if c.offset > 0 {
		f, err := c.baseFs.Open(c.path)
		if err != nil {
			return ErrCannotUseSivaFile.Wrap(err)
		}
		defer f.Close()

		if err := f.Truncate(c.offset); err != nil {
			return ErrCannotUseSivaFile.Wrap(err)

		}
	}

	return c.Reset()
}

// Save saves the current state of the siva file.
func (c *checkpoint) Save() error {
	info, err := c.baseFs.Stat(c.path)
	if err != nil {
		return ErrCannotUseSivaFile.Wrap(err)

	}

	if err := writeInt64(c.baseFs, c.persist, info.Size()); err != nil {
		return ErrCannotUseCheckpointFile.Wrap(err)
	}

	c.offset = info.Size()
	return nil
}

// Reset resets the checkpoint.
func (c *checkpoint) Reset() error {
	if err := cleanup(c.baseFs, c.persist); err != nil {
		return ErrCannotUseCheckpointFile.Wrap(err)
	}

	c.offset = 0
	return nil
}

// cleanup remove the given path from the filesystem but
// doesn't return an error in case path doesn't exist.
func cleanup(fs billy.Filesystem, path string) error {
	if err := fs.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func readInt64(fs billy.Filesystem, path string) (int64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return -1, err
	}

	data := make([]byte, 32)
	n, err := f.Read(data)
	if err != nil {
		return -1, err
	}

	num, err := strconv.ParseInt(string(data[:n]), 10, 64)
	if err != nil {
		return -1, err
	}

	if num < 0 {

		return -1, ErrMalformedData.New()
	}

	return num, nil
}

func writeInt64(fs billy.Filesystem, path string, num int64) error {
	str := strconv.FormatInt(num, 10)
	return util.WriteFile(fs, path, []byte(str), 0664)
}
