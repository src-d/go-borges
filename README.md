[![GoDoc](https://godoc.org/github.com/src-d/go-borges?status.svg)](https://godoc.org/github.com/src-d/go-borges)
[![Build Status](https://travis-ci.com/src-d/go-borges.svg)](https://travis-ci.com/src-d/go-borges)
[![codecov.io](https://codecov.io/github/src-d/go-borges/coverage.svg)](https://codecov.io/github/src-d/go-borges)
[![Go Report Card](https://goreportcard.com/badge/github.com/src-d/go-borges)](https://goreportcard.com/report/github.com/src-d/go-borges)

# go-borges

This library abstracts read and write access to a set of [go-git](https://github.com/src-d/go-git) repositories. It comes with several implementations to support different storage methods:

* `plain`: stored in the filesystem, supports transactions.
* `siva`: [rooted repositories](https://github.com/src-d/gitcollector#storing-repositories-using-rooted-repositories) in [siva files](https://github.com/src-d/go-siva), supports transactions. These files can be generated with [gitcollector](https://github.com/src-d/gitcollector).
* `legacysiva`: siva file generated by [borges](https://github.com/src-d/borges). This implementation only supports reading and does not support transactions.

When transactions are supported the writes to the repositories will be atomic and could only be seen by new readers when `Commit` function is called. That is, after opening a repository in read only mode any writes to it by another thread or process won't modify its contents. This is useful when the storage that is being used for reading repositories is being updated at the same time. More information and example in `siva` package documentation.

# Installation

`go-borges` supports go modules and can be added to your project with:

```
$ go get github.com/src-d/go-borges
```

# Example of utilization

This example lists the repositories downloaded by gitcollector.

```
package main

import (
	"fmt"
	"os"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("you need to provide the path of your siva files")
		os.Exit(1)
	}
	fs := osfs.New(os.Args[1])

	lib, err := siva.NewLibrary("library", fs, &siva.LibraryOptions{
		Bucket:        2,
		RootedRepo:    true,
		Transactional: true,
	})
	if err != nil {
		panic(err)
	}

	repos, err := lib.Repositories(borges.ReadOnlyMode)
	if err != nil {
		panic(err)
	}

	err = repos.ForEach(func(r borges.Repository) error {
		id := r.ID().String()
		head, err := r.R().Head()
		if err != nil {
			return err
		}

		fmt.Printf("repository: %v, HEAD: %v\n", id, head.Hash().String())
		return nil
	})
}
```

# Contribute

[Contributions](https://github.com/src-d/{project}/issues) are more than welcome, if you are interested please take a look to
our [Contributing Guidelines](CONTRIBUTING.md).

# Code of Conduct

All activities under source{d} projects are governed by the [source{d} code of conduct](.github/CODE_OF_CONDUCT.md).

# License

Apache License Version 2.0, see [LICENSE](LICENSE).
