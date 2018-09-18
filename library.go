package borges

import "gopkg.in/src-d/go-git.v4"

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

	//Repositories() (RepositoryIter, error)
}

type RepositoryID string

func (id RepositoryID) String() string {
	return string(id)
}

type Repository struct {
	*git.Repository

	//    Rollback() error
	//    Commit() (error)
}
