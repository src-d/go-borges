/*

Package siva implements a go-borges library that uses siva files as its
storage backend.

More information about siva files: https://github.com/src-d/go-siva

Basics

In this storage each location contains a single bare git repository that can
contain the objects from several other logical repositories. These logical
repositories are stored as remotes in the configuration file. Its ID is the
remote name but it can also search its URLs. All repositories returned from a
location have the same objects and references. The function AddLocation creates
an empty location that is initialized when using Location.Init. It's
initialization consists of initializing the repository if it's not already
created and adding a remote with its name and URLs to the provided ID.

For example:

	r1, _ := library.Get("github.com/src-d/go-borges")
	println(r1.Name()) # "0168e2c7-eedc-7358-0a09-39ba833bdd54"
	r2, _ := library.Get("0168e2c7-eedc-7358-0a09-39ba833bdd54")
	println(r1.Name()) # "0168e2c7-eedc-7358-0a09-39ba833bdd54"

	loc, _ := library.AddLocation("test")
	r1, _ := loc.Init("repo1") # the first repo initializes the git repository
	r1.Commit()
	r2, _ := loc.Init("repos2") # the second just adds a new remote
	r2.Commit()
	loc.Has("repo1") # true
	loc.Has("repo2") # true

After use of repositories they should be closed. When the library is
transactional it can be closed with Commit (only for read write mode) or Close
(save changes or rollback). When the library is non transactional it must be
closed with Close. In both cases the repository should not be used again after
closing it. A double Close returns error.

Transactions

The storage supports transactions and has location lock on transaction when
using the same library. Transactional writes are done directly to the siva file
performing appends and a checkpoint file is created with the size of the file
before starting the transaction. This file is used to recover broken siva files
to the last known good state. Locations can be accessed in read only mode while
the repository is performing a transaction and its content remain stable.

Committing a transaction finishes the writes to the siva file, closes it and
deletes the checkpoint file. Rollback truncates the siva file to the last good
size and deletes the checkpoint file.

Only one repository can be opened in read write mode in the same location when
the library is transactional. When a second repository wants to be opened in RW
mode in the same location the library will wait a grace period for the previous
repository to close. By default is 1 minute but it can be configured when
creating the library.

For example:

	loc, _ := library.Location("foo")
	r1, _ := loc.Get("github.com/src-d/go-borges", borges.ReadOnlyMode)
	r2, _ := loc.Get("github.com/src-d/go-borges", borges.RWMode)
	r2.R().CreateTag("tag", plumbing.ZeroHash, nil)

	r1.R().Tag("tag") # not found
	r3, _ := loc.Get("github.com/src-d/go-borges", borges.ReadOnlyMode)
	r3.R().Tag("tag") # not found

	# errors after configured timeout as r2 transaction is not completed
	r4, _ := loc.Get("github.com/src-d/go-borges", borges.RWMode)

	r2.Commit()
	r1.R().Tag("tag") # not found
	r5, _ := loc.Get("github.com/src-d/go-borges", borges.ReadOnlyMode)
	r5.R().Tag("tag") # found

Note: When using repositories in non transactional mode you should call Close
after finishing, otherwise the siva file will be corrupted.
*/
package siva
