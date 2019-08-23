package siva

import (
	"io/ioutil"
	"os"
	"time"

	billy "gopkg.in/src-d/go-billy.v4"
	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/ghodss/yaml"
	"github.com/google/uuid"
	"gopkg.in/src-d/go-billy.v4/util"
)

const (
	libraryMetadataFile = "library.yaml"
)

type libMetadata struct {
	// ID is the library indentifyer. It is a generated UUID id no ID is
	// provided.
	ID string `json:"id,omitempty"`
	// CurrentVersion holds the version used for reading.
	CurrentVersion int `json:"version"`

	dirty   bool
	fs      billy.Filesystem
	size    int64
	modTime time.Time
}

// loadOrCreateLibraryMetadata loads the library metadata from disk. If the data
// doesn't exist on disk it creates new data generating a new ID in case the
// passed id is an empty string..
func loadOrCreateLibraryMetadata(
	id string,
	fs billy.Filesystem,
) (*libMetadata, error) {
	m, err := loadLibraryMetadata(fs)
	if err == nil || !os.IsNotExist(err) {
		return m, err
	}

	if id == "" {
		var err error
		id, err = generateLibID()
		if err != nil {
			return nil, err
		}
	}

	return newLibraryMetadata(id, fs)
}

// newLibraryMetadata builds a new libMetadata. It persists the data on disk.
func newLibraryMetadata(
	id string,
	fs billy.Filesystem,
) (*libMetadata, error) {
	m := &libMetadata{
		ID:             id,
		CurrentVersion: -1,
		fs:             fs,
		dirty:          true,
	}

	if err := m.save(); err != nil {
		return nil, err
	}

	fi, err := fs.Stat(libraryMetadataFile)
	if err != nil {
		return nil, err
	}

	m.fs = fs
	m.modTime = fi.ModTime()
	m.size = fi.Size()

	return m, nil
}

// version returns the version stored in the library metadata file or -1
// if it's not set.
func (m *libMetadata) version() (int, error) {
	if m.dirty {
		return m.CurrentVersion, nil
	}

	fi, err := m.fs.Stat(libraryMetadataFile)
	if err != nil {
		return -1, err
	}

	if fi.ModTime() != m.modTime || fi.Size() != m.size {
		metadata, err := loadLibraryMetadata(m.fs)
		if err != nil {
			return -1, err
		}

		m.ID = metadata.ID
		m.CurrentVersion = metadata.CurrentVersion
		m.modTime = fi.ModTime()
		m.size = fi.Size()
		m.dirty = false
	}

	return m.CurrentVersion, nil
}

func (m *libMetadata) setVersion(v int) {
	if v != m.CurrentVersion {
		m.dirty = true
		m.CurrentVersion = v
	}
}

// setID changes the current version.
func (m *libMetadata) setID(id string) {
	if id != m.ID {
		m.dirty = true
		m.ID = id
	}
}

func generateLibID() (string, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	return uuid.String(), nil
}

func (m *libMetadata) save() error {
	if !m.dirty {
		return nil
	}

	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	tmp := libraryMetadataFile + ".tmp"
	defer m.fs.Remove(tmp)

	if err = util.WriteFile(m.fs, tmp, data, 0666); err != nil {
		return err
	}

	if err = m.fs.Rename(tmp, libraryMetadataFile); err != nil {
		return err
	}

	m.dirty = false
	return nil
}

func loadLibraryMetadata(fs billy.Filesystem) (*libMetadata, error) {
	mf, err := fs.Open(libraryMetadataFile)
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	data, err := ioutil.ReadAll(mf)
	if err != nil {
		return nil, err
	}

	m, err := parseLibraryMetadata(data)
	if err != nil {
		return nil, err
	}

	m.fs = fs

	fi, err := fs.Stat(libraryMetadataFile)
	if err != nil {
		return nil, err
	}

	m.modTime = fi.ModTime()
	m.size = fi.Size()

	return m, nil
}

func parseLibraryMetadata(d []byte) (*libMetadata, error) {
	var metadata libMetadata

	err := yaml.Unmarshal(d, &metadata)
	if err != nil {
		return nil, err
	}

	return &metadata, nil
}

// Version represents a valid siva file point to read from.
type Version struct {
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size,omiempty"`
}

type locationMetadata struct {
	Versions map[int]*Version `json:"versions"`

	dirty   bool
	fs      billy.Filesystem
	path    string
	size    int64
	modTime time.Time
}

const locMetadataFileExt = ".yaml"

func newLocationMetadata(
	id string,
	fs billy.Filesystem,
) *locationMetadata {
	return &locationMetadata{
		Versions: make(map[int]*Version),
		fs:       fs,
		path:     id + locMetadataFileExt,
	}
}

func parseLocationMetadata(d []byte) (*locationMetadata, error) {
	var m locationMetadata

	err := yaml.Unmarshal(d, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func loadOrCreateLocationMetadata(
	fs billy.Filesystem,
	id string,
) (*locationMetadata, error) {
	path := id + locMetadataFileExt
	m, err := loadLocationMetadata(fs, path)
	if os.IsNotExist(err) {
		return newLocationMetadata(id, fs), nil
	}

	return m, err
}

func loadLocationMetadata(
	fs billy.Filesystem,
	path string,
) (*locationMetadata, error) {
	mf, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	data, err := ioutil.ReadAll(mf)
	if err != nil {
		return nil, err
	}

	m, err := parseLocationMetadata(data)
	if err != nil {
		return nil, err
	}

	m.fs = fs
	m.path = path

	return m, nil
}

// last returns the last Version or -1 if there are no Versions.
func (m *locationMetadata) last() int {
	last := -1
	for i := range m.Versions {
		if i > last {
			last = i
		}
	}

	return last
}

// closest searches for the last Version lesser or equal to the one provided.
func (m *locationMetadata) closest(v int) int {
	closest := -1
	for i := range m.Versions {
		if i <= v && i > closest {
			closest = i
		}
	}

	return closest
}

// offset picks the closest Version from metadata and returns its offsets.
// If there are not Versions defined returns offset 0 that means to use
// the latest siva index when used with siva filesystem.
func (m *locationMetadata) offset(c int) (uint64, error) {
	version := m.last()

	if version < 0 {
		return 0, nil
	}

	b, err := m.version(c)
	if err == nil {
		return b.Offset, nil
	}

	if !errLocVersionNotExists.Is(err) {
		return 0, err
	}

	if closest := m.closest(c); closest >= 0 {
		version = closest
	}

	return m.Versions[version].Offset, nil
}

var errLocVersionNotExists = errors.NewKind("location version not exists")

// version returns information for a given version. If the version does not
// exist  an error is returned
func (m *locationMetadata) version(v int) (*Version, error) {
	if m.dirty {
		Version, ok := m.Versions[v]
		if !ok {
			return nil, errLocVersionNotExists.New()
		}

		return Version, nil
	}

	fi, err := m.fs.Stat(m.path)
	if err != nil {
		return nil, err
	}

	if fi.ModTime() != m.modTime || fi.Size() != m.size {
		metadata, err := loadLocationMetadata(m.fs, m.path)
		if err != nil {
			return nil, err
		}

		m.Versions = metadata.Versions
		m.modTime = fi.ModTime()
		m.size = fi.Size()
		m.dirty = false
	}

	Version, ok := m.Versions[v]
	if !ok {
		return nil, errLocVersionNotExists.New()
	}

	return Version, nil
}

func (m *locationMetadata) setVersion(n int, b *Version) {
	current, ok := m.Versions[n]
	if ok && (current.Offset == b.Offset && current.Size == b.Size) {
		return
	}

	m.Versions[n] = b
	m.dirty = true
}

// deleteVersion deletes a given version.
func (m *locationMetadata) deleteVersion(n int) {
	_, ok := m.Versions[n]
	if !ok {
		return
	}

	delete(m.Versions, n)
	m.dirty = true
}

func (m *locationMetadata) save() error {
	if !m.dirty {
		return nil
	}

	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	tmp := m.path + ".tmp"
	defer m.fs.Remove(tmp)

	err = util.WriteFile(m.fs, tmp, data, 0666)
	if err != nil {
		return err
	}

	return m.fs.Rename(tmp, m.path)
}
