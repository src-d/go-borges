package siva

import (
	"fmt"
	"strings"

	"io"

	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
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
	c, ok := r.Storer.(io.Closer)
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

// IterEncodedObjects implements EncodedObjectStorer interface.
func (r *RootedStorage) IterEncodedObjects(
	t plumbing.ObjectType,
) (storer.EncodedObjectIter, error) {
	switch t {
	case plumbing.CommitObject:
		return r.commitObjects()

	case plumbing.TreeObject:
		return r.treeObjects()

	case plumbing.BlobObject:
		return r.blobObjects()

	default:
		return r.Storer.IterEncodedObjects(t)
	}
}

func (r *RootedStorage) commitObjects() (storer.EncodedObjectIter, error) {
	commits, err := r.commitIter()
	if err != nil {
		return nil, err
	}

	return &commitObjectIter{
		commits: commits,
		sto:     r,
	}, nil
}

func (r *RootedStorage) commitIter() (*commitIter, error) {
	refs, err := r.IterReferences()
	if err != nil {
		return nil, err
	}

	return &commitIter{
		sto:  r,
		refs: refs,
		seen: make(map[plumbing.Hash]bool),
	}, nil
}

func (r *RootedStorage) treeObjects() (storer.EncodedObjectIter, error) {
	trees, err := r.treeIter()
	if err != nil {
		return nil, err
	}

	return &treeObjectIter{
		trees: trees,
		sto:   r,
	}, nil
}

func (r *RootedStorage) treeIter() (*treeIter, error) {
	commits, err := r.commitIter()
	if err != nil {
		return nil, err
	}

	return &treeIter{
		commits: commits,
		sto:     r,
		seen:    make(map[plumbing.Hash]struct{}),
	}, nil
}

func (r *RootedStorage) blobObjects() (storer.EncodedObjectIter, error) {
	trees, err := r.treeIter()
	if err != nil {
		return nil, err
	}

	return &blobObjectIter{
		trees: trees,
		sto:   r,
		seen:  make(map[plumbing.Hash]struct{}),
	}, nil
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

// commitIter iterates all the commits reachable by the repository
// references. It returns *object.Commit.
type commitIter struct {
	sto  *RootedStorage
	refs storer.ReferenceIter
	seen map[plumbing.Hash]bool
	log  object.CommitIter
}

func (c *commitIter) Next() (*object.Commit, error) {
	var err error
	var ref *plumbing.Reference
	var commit *object.Commit

	for {
		if c.log == nil {
			ref, err = c.refs.Next()
			if err != nil {
				return nil, err
			}

			if _, ok := c.seen[ref.Hash()]; ok {
				continue
			}

			commit, err := object.GetCommit(c.sto, ref.Hash())
			if err == plumbing.ErrObjectNotFound {
				continue
			}

			if err != nil {
				return nil, err
			}

			c.log = object.NewCommitPreorderIter(commit, c.seen, nil)
		}

		commit, err = c.log.Next()
		if err == io.EOF {
			if c.log != nil {
				c.log.Close()
				c.log = nil
			}
			continue
		}
		if err != nil {
			return nil, err
		}

		c.seen[commit.Hash] = true

		return commit, nil
	}
}

func (c *commitIter) ForEach(f func(*object.Commit) error) error {
	defer c.Close()
	for {
		commit, err := c.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = f(commit)
		if err != nil {
			return err
		}
	}
}

func (c *commitIter) Close() {
	if c != nil {
		if c.log != nil {
			c.log.Close()
		}

		if c.refs != nil {
			c.refs.Close()
		}
	}
}

type commitObjectIter struct {
	commits *commitIter
	sto     *RootedStorage
}

func (c *commitObjectIter) Next() (plumbing.EncodedObject, error) {
	for {
		commit, err := c.commits.Next()
		if err != nil {
			return nil, err
		}

		obj, err := c.sto.EncodedObject(plumbing.CommitObject, commit.Hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		return obj, err
	}
}

func (c *commitObjectIter) ForEach(f func(plumbing.EncodedObject) error) error {
	return encodedObjectIterForEach(c, f)
}

func (c *commitObjectIter) Close() {
	c.commits.Close()
}

type treeIter struct {
	commits *commitIter
	sto     *RootedStorage
	seen    map[plumbing.Hash]struct{}
	walker  *object.TreeWalker
	queue   []plumbing.Hash
	entries []object.TreeEntry
}

func (t *treeIter) Next() (*object.Tree, error) {
	for {
		if len(t.entries) == 0 {
			if len(t.queue) == 0 {
				commit, err := t.commits.Next()
				if err == plumbing.ErrObjectNotFound {
					continue
				}
				if err != nil {
					return nil, err
				}

				tree, err := commit.Tree()
				if err != nil {
					return nil, err
				}

				if _, ok := t.seen[tree.Hash]; ok {
					continue
				}

				t.entries = tree.Entries
				t.seen[tree.Hash] = struct{}{}

				return tree, err
			}

			tree, err := object.GetTree(t.sto, t.queue[0])
			t.queue = t.queue[1:]

			if err == plumbing.ErrObjectNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}

			if _, ok := t.seen[tree.Hash]; ok {
				continue
			}

			t.entries = tree.Entries
			t.seen[tree.Hash] = struct{}{}

			return tree, err
		}

		entry := t.entries[0]
		t.entries = t.entries[1:]

		if entry.Mode.IsFile() {
			continue
		}

		if _, ok := t.seen[entry.Hash]; ok {
			continue
		}
		t.seen[entry.Hash] = struct{}{}

		tree, err := object.GetTree(t.sto, entry.Hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		t.queue = append(t.queue, entry.Hash)
		return tree, nil
	}
}

func (t *treeIter) ForEach(f func(*object.Tree) error) error {
	defer t.Close()
	for {
		tree, err := t.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = f(tree)
		if err != nil {
			return err
		}
	}
}

func (t *treeIter) Close() {
	if t != nil {
		if t.walker != nil {
			t.walker.Close()
		}

		if t.commits != nil {
			t.commits.Close()
		}
	}
}

type treeObjectIter struct {
	trees *treeIter
	sto   *RootedStorage
}

func (c *treeObjectIter) Next() (plumbing.EncodedObject, error) {
	for {
		tree, err := c.trees.Next()
		if err != nil {
			return nil, err
		}

		obj, err := c.sto.EncodedObject(plumbing.TreeObject, tree.Hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		return obj, err
	}
}

func (c *treeObjectIter) ForEach(f func(plumbing.EncodedObject) error) error {
	return encodedObjectIterForEach(c, f)
}

func (c *treeObjectIter) Close() {
	c.trees.Close()
}

type blobObjectIter struct {
	trees   *treeIter
	sto     *RootedStorage
	seen    map[plumbing.Hash]struct{}
	entries []object.TreeEntry
}

func (b *blobObjectIter) Next() (plumbing.EncodedObject, error) {
	for {
		if len(b.entries) == 0 {
			tree, err := b.trees.Next()
			if err != nil {
				return nil, err
			}

			b.entries = tree.Entries
			continue
		}

		entry := b.entries[0]
		b.entries = b.entries[1:]

		if !entry.Mode.IsFile() {
			continue
		}

		if _, ok := b.seen[entry.Hash]; ok {
			continue
		}
		b.seen[entry.Hash] = struct{}{}

		obj, err := b.sto.EncodedObject(plumbing.BlobObject, entry.Hash)
		if err == plumbing.ErrObjectNotFound {
			continue
		}

		return obj, err
	}
}

func (b *blobObjectIter) ForEach(f func(plumbing.EncodedObject) error) error {
	return encodedObjectIterForEach(b, f)
}

func (b *blobObjectIter) Close() {
	b.trees.Close()
}

func encodedObjectIterForEach(
	i storer.EncodedObjectIter,
	f func(plumbing.EncodedObject) error,
) error {
	defer i.Close()
	for {
		tree, err := i.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = f(tree)
		if err != nil {
			return err
		}
	}
}
