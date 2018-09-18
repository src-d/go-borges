package plain

import (
	"os"

	"gopkg.in/src-d/go-billy.v4"
)

var requiredGitPaths = []string{"HEAD", "objects", "refs/heads"}

// IsRepository return true if the given path in the given folder contains a
// valid repository, isBare is true when the given path contains a bare
// repository.
//
// The identifciation method is based on the stat of 3 different files/folder,
// cgit, makes a extra validation in the content on the HEAD file.
func IsRepository(fs billy.Filesystem, path string) (isRepository, isBare bool, err error) {
	isRepository, err = isDotGitRepository(fs, path)
	if err != nil {
		return
	}

	if isRepository {
		isBare = true
		return
	}

	isRepository, err = isDotGitRepository(fs, fs.Join(path, ".git"))
	return
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
