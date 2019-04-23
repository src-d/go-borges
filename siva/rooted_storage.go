package siva

import (
	"fmt"
	"strings"

	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage"
)

const (
	refsBase    = "refs/"
	remotesBase = "refs/remotes/"
	rootedHEAD  = "refs/HEAD"
)

// RootedStorage returns only the references and objects from a repository
// stored as a remote in a rooted repository.
type RootedStorage struct {
	storage.Storer
	id string
}

// NewRootedStorage creates a new storer that only shows references from
// an specific remote ID.
func NewRootedStorage(s storage.Storer, id string) *RootedStorage {
	return &RootedStorage{
		Storer: s,
		id:     id,
	}
}

// Commit implements Committer interface.
func (r *RootedStorage) Commit() error {
	c, ok := r.Storer.(Committer)
	if ok {
		return c.Commit()
	}

	return nil
}

// Close implements Committer interface.
func (r *RootedStorage) Close() error {
	c, ok := r.Storer.(Committer)
	if ok {
		return c.Close()
	}

	return nil
}

func (r *RootedStorage) refPrefix() string {
	return remotesBase + r.id + "/"
}

func (r *RootedStorage) convertReferenceNameToRemote(
	ref plumbing.ReferenceName,
) plumbing.ReferenceName {
	return r.convertReferenceName(ref, "refs/", r.refPrefix())
}

func (r *RootedStorage) convertReferenceNameFromRemote(
	ref plumbing.ReferenceName,
) plumbing.ReferenceName {
	name := r.convertReferenceName(ref, r.refPrefix(), "refs/")
	if name == "refs/"+plumbing.HEAD {
		return plumbing.HEAD
	}

	return name
}

func (r *RootedStorage) convertReferenceName(
	ref plumbing.ReferenceName,
	from string,
	to string,
) plumbing.ReferenceName {
	if r.id == "" {
		return ref
	}

	base := strings.TrimPrefix(string(ref), from)
	name := fmt.Sprintf("%s%s", to, base)

	return plumbing.ReferenceName(name)
}

func (r *RootedStorage) convertReferenceToRemote(
	ref *plumbing.Reference,
) (*plumbing.Reference, error) {
	return r.convertReference(ref, "refs/", r.refPrefix())
}

func (r *RootedStorage) convertReferenceFromRemote(
	ref *plumbing.Reference,
) (*plumbing.Reference, error) {
	newRef, err := r.convertReference(ref, r.refPrefix(), "refs/")
	if err != nil {
		return nil, err
	}

	if newRef.Name() == "refs/HEAD" {
		return r.convertReference(ref, r.refPrefix(), "")
	}

	return newRef, nil
}

func (r *RootedStorage) convertReference(
	ref *plumbing.Reference,
	from string,
	to string,
) (*plumbing.Reference, error) {
	if ref == nil {
		return nil, nil
	}

	name := r.convertReferenceName(ref.Name(), from, to)

	var newRef *plumbing.Reference
	switch ref.Type() {
	case plumbing.SymbolicReference:
		newRef = plumbing.NewSymbolicReference(name, ref.Target())

	case plumbing.HashReference:
		newRef = plumbing.NewHashReference(name, ref.Hash())

	default:
		return nil, plumbing.ErrInvalidType
	}

	return newRef, nil
}

// SetReference implements ReferenceStorer interface.
func (r *RootedStorage) SetReference(ref *plumbing.Reference) error {
	n, err := r.convertReferenceToRemote(ref)
	if err != nil {
		return err
	}

	return r.Storer.SetReference(n)
}

// CheckAndSetReference implements ReferenceStorer interface.
func (r *RootedStorage) CheckAndSetReference(
	new *plumbing.Reference,
	old *plumbing.Reference,
) error {
	n, err := r.convertReferenceToRemote(new)
	if err != nil {
		return err
	}

	o, err := r.convertReferenceToRemote(old)
	if err != nil {
		return err
	}

	return r.Storer.CheckAndSetReference(n, o)
}

// Reference implements ReferenceStorer interface.
func (r *RootedStorage) Reference(
	name plumbing.ReferenceName,
) (*plumbing.Reference, error) {
	ref, err := r.Storer.Reference(r.convertReferenceNameToRemote(name))
	if err != nil {
		return nil, err
	}

	return r.convertReferenceFromRemote(ref)
}

// IterReferences implements ReferenceStorer interface.
func (r *RootedStorage) IterReferences() (storer.ReferenceIter, error) {
	iter, err := r.Storer.IterReferences()
	if err != nil {
		return nil, err
	}

	return &refIter{
		iter:   iter,
		prefix: r.refPrefix(),
		sto:    r,
	}, nil
}

// RemoveReference implements ReferenceStorer interface.
func (r *RootedStorage) RemoveReference(ref plumbing.ReferenceName) error {
	n := r.convertReferenceNameToRemote(ref)
	return r.Storer.RemoveReference(n)
}

type refIter struct {
	iter   storer.ReferenceIter
	prefix string
	sto    *RootedStorage
}

func (r *refIter) Next() (*plumbing.Reference, error) {
	for {
		ref, err := r.iter.Next()
		if err != nil {
			return nil, err
		}

		name := string(ref.Name())
		if strings.HasPrefix(name, r.prefix) {
			return r.sto.convertReferenceFromRemote(ref)
		}
	}
}

func (r *refIter) ForEach(f func(*plumbing.Reference) error) error {
	defer r.Close()
	for {
		ref, err := r.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = f(ref)
		if err != nil {
			return err
		}
	}
}

func (r *refIter) Close() {
	r.iter.Close()
}
