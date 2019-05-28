package siva

import (
	"io/ioutil"
	"os"

	"github.com/ghodss/yaml"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
)

const (
	// LibraryMetadataFile is the name of the file that holds library metadata.
	LibraryMetadataFile = "library.yaml"
)

// LibraryMetadata holds information about the library.
type LibraryMetadata struct {
	CurrentVersion int `json:"version"`

	dirty bool
}

// NewLibraryMetadata creates a new LibraryMetadata.
func NewLibraryMetadata(Version int) *LibraryMetadata {
	return &LibraryMetadata{
		CurrentVersion: Version,
	}
}

// Version returns the version stored in the library metadata file or -1
// if it's not set.
func (m *LibraryMetadata) Version() int {
	if m == nil {
		return -1
	}

	return m.CurrentVersion
}

// SetVersion changes the current version.
func (m *LibraryMetadata) SetVersion(v int) {
	if v != m.CurrentVersion {
		m.dirty = true
		m.CurrentVersion = v
	}
}

// Save writes metadata to the library yaml file.
func (m *LibraryMetadata) Save(fs billy.Filesystem) error {
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	tmp := LibraryMetadataFile + ".tmp"
	defer fs.Remove(tmp)

	err = util.WriteFile(fs, tmp, data, 0666)
	if err != nil {
		return err
	}

	return fs.Rename(tmp, LibraryMetadataFile)
}

// Dirty returns true if the metadata was changed and it needs to be written.
func (m *LibraryMetadata) Dirty() bool {
	return m.dirty
}

// parseLibraryMetadata parses the yaml representation of library metadata.
func parseLibraryMetadata(d []byte) (*LibraryMetadata, error) {
	var m LibraryMetadata

	err := yaml.Unmarshal(d, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// loadLibraryMetadata reads and parses a library metadata file.
func loadLibraryMetadata(fs billy.Filesystem) (*LibraryMetadata, error) {
	mf, err := fs.Open(LibraryMetadataFile)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	data, err := ioutil.ReadAll(mf)
	if err != nil {
		return nil, err
	}

	return parseLibraryMetadata(data)
}

// Version describes a bookmark in the siva file.
type Version struct {
	// Offset is a position in the siva file.
	Offset uint64 `json:"offset"`
	// Size is block size of the Version.
	Size uint64 `json:"size,omiempty"`
}

// LocationMetadata holds extra data associated with a siva file.
type LocationMetadata struct {
	// Versions holds a numbered list of bookmarks in the siva file.
	Versions map[int]Version `json:"versions"`

	dirty bool
}

// NewLocationMetadata creates a new LocationMetadata.
func NewLocationMetadata(versions map[int]Version) *LocationMetadata {
	return &LocationMetadata{
		Versions: versions,
	}
}

// parseLocationMetadata parses the yaml representation of location metadata.
func parseLocationMetadata(d []byte) (*LocationMetadata, error) {
	var m LocationMetadata

	err := yaml.Unmarshal(d, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// loadLocationMetadata reads and parses a location metadata file.
func loadLocationMetadata(
	fs billy.Filesystem,
	path string,
) (*LocationMetadata, error) {
	mf, err := fs.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer mf.Close()

	data, err := ioutil.ReadAll(mf)
	if err != nil {
		return nil, err
	}

	return parseLocationMetadata(data)
}

// locationMetadataPath returns the path for a location metadata file.
func locationMetadataPath(path string) string {
	return path + ".yaml"
}

// Last returns the last Version or -1 if there are no Versions.
func (m *LocationMetadata) Last() int {
	if m == nil {
		return -1
	}

	last := -1
	for i := range m.Versions {
		if i > last {
			last = i
		}
	}

	return last
}

// closest searches for the last Version lesser or equal to the one provided.
func (m *LocationMetadata) closest(v int) int {
	closest := -1
	for i := range m.Versions {
		if i <= v && i > closest {
			closest = i
		}
	}

	return closest
}

// Offset picks the closest Version from metadata and returns its offsets.
// If there are not Versions defined returns offset 0 that means to use
// the latest siva index when used with siva filesystem.
func (m *LocationMetadata) Offset(c int) uint64 {
	Version := m.Last()

	if Version < 0 {
		return 0
	}

	if v, ok := m.Versions[c]; ok {
		return v.Offset
	}

	if closest := m.closest(c); closest >= 0 {
		Version = closest
	}

	return m.Versions[Version].Offset
}

// Version returns information for a given version. Second return argument
// is false if the version does not exist.
func (m *LocationMetadata) Version(v int) (Version, bool) {
	if m == nil {
		return Version{}, false
	}

	d, ok := m.Versions[v]
	return d, ok
}

// SetVersion changes or adds the information for a version.
func (m *LocationMetadata) SetVersion(n int, v Version) {
	Version, ok := m.Versions[n]
	if ok && Version == v {
		return
	}

	m.Versions[n] = v
	m.dirty = true
}

// DeleteVersion deletes a given version.
func (m *LocationMetadata) DeleteVersion(n int) {
	_, ok := m.Versions[n]
	if !ok {
		return
	}

	delete(m.Versions, n)
	m.dirty = true
}

// Dirty returns true if metadata was changed and needs saving.
func (m *LocationMetadata) Dirty() bool {
	return m.dirty
}

// Save writes metadata to the yaml file for the give siva path.
func (m *LocationMetadata) Save(fs billy.Filesystem, path string) error {
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	path = locationMetadataPath(path)
	tmp := path + ".tmp"
	defer fs.Remove(tmp)

	err = util.WriteFile(fs, tmp, data, 0666)
	if err != nil {
		return err
	}

	return fs.Rename(tmp, path)
}
