package borges

import (
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage"
)

type Library interface {
	Location
	Location(id LocationID) Location
	//Locations() (LocationIter, error)
}

type Mode int
type LocationID string

type Location interface {
	GetOrInit(id RepositoryID, mode Mode) (*Repository, error)
	Init(id RepositoryID, mode Mode) (*Repository, error)
	Has(id RepositoryID) (bool, error)
	Get(id RepositoryID, mode Mode) (*Repository, error)

	Repositories() (RepositoryIterator, error)
}

type RepositoryID string

func (id RepositoryID) String() string {
	return string(id)
}

func OpenRepository(id RepositoryID, s storage.Storer, worktree billy.Filesystem) (*Repository, error) {
	r, err := git.Open(s, worktree)
	if err != nil {
		return nil, err
	}

	return &Repository{id, r}, nil
}

func InitRepository(id RepositoryID, s storage.Storer, worktree billy.Filesystem) (*Repository, error) {
	r, err := git.Init(s, worktree)
	if err != nil {
		return nil, err
	}

	return &Repository{id, r}, nil
}

type Repository struct {
	ID RepositoryID
	*git.Repository

	//    Rollback() error
	//    Commit() (error)
}
