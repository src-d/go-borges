package plain

import (
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/util"

	billy "gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
	"gopkg.in/src-d/go-git.v4/storage/transactional"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
)

// Repository represents a git plain repository.
type Repository struct {
	id           borges.RepositoryID
	l            *Location
	mode         borges.Mode
	temporalPath string

	*git.Repository
}

func initRepository(l *Location, id borges.RepositoryID) (*Repository, error) {
	s, tempPath, err := repositoryStorer(l, id, borges.RWMode)
	if err != nil {
		return nil, err
	}

	r, err := git.Init(s, nil)
	if err != nil {
		return nil, err
	}

	_, err = r.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{id.String()},
	})

	if err != nil {
		return nil, err
	}

	return &Repository{
		id:           id,
		l:            l,
		mode:         borges.RWMode,
		temporalPath: tempPath,
		Repository:   r,
	}, nil
}

// openRepository, is the basic operation of open a repository without any checking.
func openRepository(l *Location, id borges.RepositoryID, mode borges.Mode) (*Repository, error) {
	s, tempPath, err := repositoryStorer(l, id, mode)
	if err != nil {
		return nil, err
	}

	r, err := git.Open(s, nil)
	if err != nil {
		return nil, err
	}

	return &Repository{
		id:           id,
		l:            l,
		mode:         mode,
		temporalPath: tempPath,
		Repository:   r,
	}, nil
}

func repositoryStorer(l *Location, id borges.RepositoryID, mode borges.Mode) (
	s storage.Storer, tempPath string, err error) {

	fs, err := l.fs.Chroot(l.RepositoryPath(id))
	if err != nil {
		return nil, "", err
	}

	s = filesystem.NewStorage(fs, cache.NewObjectLRUDefault())

	switch mode {
	case borges.ReadOnlyMode:
		return &util.ReadOnlyStorer{s}, "", nil
	case borges.RWMode:
		if l.opts.Transactional {
			return repositoryTemporalStorer(l, id, s)
		}

		return s, "", nil
	default:
		return nil, "", borges.ErrModeNotSupported.New(mode)
	}
}

func repositoryTemporalStorer(l *Location, id borges.RepositoryID, parent storage.Storer) (
	s storage.Storer, tempPath string, err error) {

	tempPath, err = billy.TempDir(l.opts.TemporalFilesystem, "transactions", "")
	if err != nil {
		return nil, "", err
	}

	fs, err := l.opts.TemporalFilesystem.Chroot(tempPath)
	if err != nil {
		return nil, "", err
	}

	ts := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	s = transactional.NewStorage(parent, ts)

	return
}

// R returns the git.Repository.
func (r *Repository) R() *git.Repository {
	return r.Repository
}

// ID returns the RepositoryID.
func (r *Repository) ID() borges.RepositoryID {
	return r.id
}

// LocationID returns the LocationID from the Location where it was retrieved.
func (r *Repository) LocationID() borges.LocationID {
	return r.l.ID()
}

// Mode returns the Mode how it was opened.
func (r *Repository) Mode() borges.Mode {
	return r.mode
}

// Close closes the repository, if the repository was opened in transactional
// Mode, will delete any write operation pending to be written.
func (r *Repository) Close() error {
	if !r.l.opts.Transactional {
		return nil
	}

	return r.cleanupTemporal()
}

func (r *Repository) cleanupTemporal() error {
	return billy.RemoveAll(r.l.opts.TemporalFilesystem, r.temporalPath)
}

// Commit persists all the write operations done since was open, if the
// repository wasn't opened in a Location with Transactions enable returns
// ErrNonTransactional.
func (r *Repository) Commit() (err error) {
	if !r.l.opts.Transactional {
		return borges.ErrNonTransactional.New()
	}

	defer ioutil.CheckClose(r, &err)
	ts, ok := r.Storer.(transactional.Storage)
	if !ok {
		panic("unreachable code")
	}

	err = ts.Commit()
	return
}
