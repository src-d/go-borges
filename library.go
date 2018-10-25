package borges

import (
	"path"
	"strings"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-git.v4/storage"
)

type Library interface {
	Location
	Location(id LocationID) Location
	//Locations() (LocationIter, error)
}

type Mode int

const (
	RWMode Mode = iota
	ReadOnlyMode
)

type LocationID string

type Location interface {
	GetOrInit(RepositoryID) (*Repository, error)
	Init(RepositoryID) (*Repository, error)
	Has(RepositoryID) (bool, error)
	Get(RepositoryID, Mode) (*Repository, error)
	Repositories(Mode) (RepositoryIterator, error)
}

type RepositoryID string

// NewRepositoryID returns a new RepositoryID based on a given endpoint.
// Eg.: git@github.com:src-d/go-borges becomes github.com/src-d/go-borges.git
func NewRepositoryID(endpoint string) (RepositoryID, error) {
	e, err := transport.NewEndpoint(endpoint)
	if err != nil {
		return "", err
	}

	if !strings.HasSuffix(e.Path, ".git") {
		e.Path += ".git"
	}

	return RepositoryID(path.Join(e.Host, e.Path)), nil
}

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
