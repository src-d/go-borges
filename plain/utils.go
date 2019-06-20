package plain

import (
	"os"
	"path/filepath"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
)

var requiredGitPaths = []string{"HEAD", "objects", "refs/heads"}

// IsRepository return true if the given path in the given filesystem contains a
// valid repository.
//
// The identifciation method is based on the stat of 3 different files/folder,
// cgit, makes a extra validation in the content on the HEAD file.
func IsRepository(fs billy.Filesystem, path string, isBare bool) (bool, error) {
	if !isBare {
		path = fs.Join(path, ".git")
	}

	return isDotGitRepository(fs, path)
}

func isDotGitRepository(fs billy.Filesystem, path string) (bool, error) {
	for _, p := range requiredGitPaths {
		_, err := fs.Stat(fs.Join(path, p))
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}

			return false, err
		}
	}

	return true, nil
}

var (
	// ErrRepositoriesNotFound is returned when there's no any repository in
	// a certain directory.
	ErrRepositoriesNotFound = errors.NewKind("couldn't find any repository")
)

// IsFirstRepositoryBare walks the given path containing repositories, checking
// if the first found repository is bare. If it can't find repositories an
// ErrRepositoriesNotFound will be returned.
func IsFirstRepositoryBare(fs billy.Filesystem, path string) (bool, error) {
	entries, err := fs.ReadDir(path)
	if err != nil {
		return false, err
	}

	if len(entries) == 0 {
		return false, ErrRepositoriesNotFound.New()
	}

	for _, e := range entries {
		if e.IsDir() {
			p := filepath.Join(path, e.Name())
			ok, err := IsRepository(fs, p, true)
			if err != nil {
				return false, err
			}

			if ok {
				return true, nil
			}

			ok, err = IsRepository(fs, p, false)
			if err != nil {
				return false, err
			}

			if ok {
				return false, nil
			}

			ok, err = IsFirstRepositoryBare(fs, p)
			if ErrRepositoriesNotFound.Is(err) {
				continue
			}

			if err != nil {
				return false, err
			}

			return ok, nil
		}
	}

	return false, ErrRepositoriesNotFound.New()
}
