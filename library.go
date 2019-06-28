package borges

import (
	"path"
	"strings"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
)

var (
	// ErrNotImplemented is returned by method of any implementation that are
	// not implemented on this specific implementation.
	ErrNotImplemented = errors.NewKind("not implemented")
	// ErrModeNotSupported is returned in the case of a request to open a
	// repository with a Mode not supported.
	ErrModeNotSupported = errors.NewKind("repository mode %q not supported")
	// ErrLocationNotExists when a Location is requested and can't be found.
	ErrLocationNotExists = errors.NewKind("location %s not exists")
	// ErrLibraryNotExists when a Library is requested and can't be found.
	ErrLibraryNotExists = errors.NewKind("library %s not exists")
	// ErrRepositoryExists an error returned on a request of Init on a location
	// with a repository with this RepositoryID already exists.
	ErrRepositoryExists = errors.NewKind("repository %s already exists")
	// ErrRepositoryNotExists when a Repository is requested and can't be found.
	ErrRepositoryNotExists = errors.NewKind("repository %s not exists")
	// ErrNonTransactional returned when Repository.Commit is called on a
	// repository that not support transactions.
	ErrNonTransactional = errors.NewKind("non transactional repository")
)

// RepositoryID represents a Repository identifier, these IDs regularly are
// based on a http or git remore URL, but can be based on any other concept.
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

// Mode is the different modes to open a Repository.
type Mode int

const (
	// RWMode allows to perform read and write operations over a repository.
	RWMode Mode = iota
	// ReadOnlyMode allows only read-only operations over a repository.
	ReadOnlyMode
)

// Repository interface represents a git.Repository, with information about
// how it was open and where is located. Also provides Transactional
// capabilities through the method Commit.
type Repository interface {
	// ID returns the RepositoryID.
	ID() RepositoryID
	// LocationID returns the LocationID from the Location where it was retrieved.
	LocationID() LocationID
	// Mode returns the Mode how it was opened.
	Mode() Mode
	// Commit persists all the write operations done since was open, if the
	// repository doesn't provide Transactional capabilities should return
	// ErrNonTransactional.
	Commit() error
	// Close closes the repository, if the repository was opened in transactional
	// Mode, will delete any write operation pending to be written.
	Close() error
	// R returns the git.Repository.
	R() *git.Repository
	// FS returns the filesystem to read or write directly to the repository or
	// nil if not available.
	FS() billy.Filesystem
}

// LibraryID represents a Library identifier.
type LibraryID string

// Library interface represents a group of different libraries and locations,
// it allows access to any repository stored on any library or location. Also
// allows the iteration of the libraries and locations to perform full scan
// operations. Library is the default entrypoint for accessing the repositories,
// should be used when the Location is not important.
type Library interface {
	// ID returns the LibraryID for this Library.
	ID() LibraryID
	// Init initializes a new Repository in a Location, the chosen Location
	// is dependant on the implementation, if this this not supported should
	// return ErrNotImplemented. If a repository with the given RepositoryID
	// already exists ErrRepositoryExists is returned.
	Init(RepositoryID) (Repository, error)
	// Get open a repository with the given RepositoryID, it itereates all the
	// library locations until this repository is found. If a repository with
	// the given RepositoryID can't be found the ErrRepositoryNotExists is
	// returned.
	Get(RepositoryID, Mode) (Repository, error)
	// GetOrInit open or initilizes a Repository at a Location, if this this not
	// supported should return ErrNotImplemented. If the repository is opened
	// this will be done in RWMode.
	GetOrInit(RepositoryID) (Repository, error)
	// Has returns true, the LibraryID and the LocationID if the given
	// RepositoryID matches any repository at any location belonging to this
	// Library.
	Has(RepositoryID) (bool, LibraryID, LocationID, error)
	// Repositories returns a RepositoryIterator that iterates through all
	// the repositories contained in all Location contained in this Library.
	Repositories(Mode) (RepositoryIterator, error)
	// Location returns the Location with the given LocationID, if a location
	// can't be found ErrLocationNotExists is returned.
	Location(LocationID) (Location, error)
	// Locations returns a LocationIterator that iterates through all locations
	// contained in this Library.
	Locations() (LocationIterator, error)
	// Library returns the Library with the given LibraryID, if a library can't
	// be found ErrLibraryNotExists is returned.
	Library(LibraryID) (Library, error)
	// Libraries returns a LibraryIterator that iterates through all libraries
	// contained in this Library.
	Libraries() (LibraryIterator, error)
}

// LocationID represents a Location identifier.
type LocationID string

// Location interface represents a physical location where the repositories are
// stored, it allows access only to the repositories contained in this location.
type Location interface {
	// ID returns the LocationID for this Location.
	ID() LocationID
	// Init initializes a new Repository at this Location.
	Init(RepositoryID) (Repository, error)
	// Get open a repository with the given RepositoryID, this operation doesn't
	// perform any read operation. If a repository with the given RepositoryID
	// already exists ErrRepositoryExists is returned.
	Get(RepositoryID, Mode) (Repository, error)
	// GetOrInit open or initilizes a Repository at this Location. If a
	// repository with the given RepositoryID can't be found the
	// ErrRepositoryNotExists is returned. If the repository is opened this will
	// be done in RWMode.
	GetOrInit(RepositoryID) (Repository, error)
	// Has returns true if the given RepositoryID matches any repository at
	// this location.
	Has(RepositoryID) (bool, error)
	// Repositories returns a RepositoryIterator that iterates through all
	// the repositories contained in this Location.
	Repositories(Mode) (RepositoryIterator, error)
}
