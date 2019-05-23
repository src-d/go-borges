package siva

import (
	"io/ioutil"
	"os"

	"github.com/ghodss/yaml"
	billy "gopkg.in/src-d/go-billy.v4"
)

const (
	// LibraryMetadataFile is the name of the file that holds library metadata.
	LibraryMetadataFile = "library.yaml"
)

// LibraryMetadata holds information about the library.
type LibraryMetadata struct {
	Version int `json:"version"`
}

// ParseLibraryMetadata parses the yaml representation of library metadata.
func ParseLibraryMetadata(d []byte) (*LibraryMetadata, error) {
	var m LibraryMetadata

	err := yaml.Unmarshal(d, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// LoadLibraryMetadata reads and parses a library metadata file.
func LoadLibraryMetadata(fs billy.Filesystem) (*LibraryMetadata, error) {
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

	return ParseLibraryMetadata(data)
}

// Version describes a bookmark in the siva file.
type Version struct {
	// Offset is a position in the siva file.
	Offset uint64 `json:"offset"`
	// Size is block size of the version.
	Size uint64 `json:"size,omiempty"`
}

// Versions holds a numbered list of bookmarks in the siva file.
type Versions map[int]Version

// LocationMetadata holds extra data associated with a siva file.
type LocationMetadata struct {
	Versions Versions `json:"versions"`
}

// ParseLocationMetadata parses the yaml representation of location metadata.
func ParseLocationMetadata(d []byte) (*LocationMetadata, error) {
	var m LocationMetadata

	err := yaml.Unmarshal(d, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// LoadLocationMetadata reads and parses a location metadata file.
func LoadLocationMetadata(fs billy.Filesystem, path string) (*LocationMetadata, error) {
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

	return ParseLocationMetadata(data)
}

// LocationMetadataPath returns the path for a location metadata file.
func LocationMetadataPath(path string) string {
	return path + ".yaml"
}

// ToYaml returns the yaml representation of the location metadata.
func (m *LocationMetadata) ToYaml() ([]byte, error) {
	return yaml.Marshal(m)
}

// Last returns the last version or -1 if there are no versions.
func (m *LocationMetadata) Last() int {
	last := -1
	for i := range m.Versions {
		if i > last {
			last = i
		}
	}

	return last
}

// closest searches for the last version lesser or equal to the one provided.
func (m *LocationMetadata) closest(v int) int {
	closest := -1
	for i := range m.Versions {
		if i <= v && i > closest {
			closest = i
		}
	}

	return closest
}

// OffsetFromLibrary picks the version from Library metadata and returns its
// offsets. If the version does not exist it selects the closest previous
// version. In case there's no Library metadata it picks the latest version.
// If there are not versions defined returns offset 0 that means to use
// the latest siva index when used with siva filesystem.
func (m *LocationMetadata) OffsetFromLibrary(l *LibraryMetadata) uint64 {
	version := m.Last()

	if version == -1 {
		return 0
	}

	if l != nil {
		if v, ok := m.Versions[l.Version]; ok {
			return v.Offset
		}

		if closest := m.closest(l.Version); closest >= 0 {
			version = closest
		}
	}

	return m.Versions[version].Offset
}
