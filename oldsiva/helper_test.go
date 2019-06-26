package oldsiva

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/util"

	"github.com/stretchr/testify/require"
)

/*
_testdata

	3974996807a9f596cf25ac3a714995c24bb97e2c.siva
	- repo: github.com/rtyley/small-test-repo
		- commits from master: 2
		- total commits in the repo: 13
	- forked repo: github.com/kuldeep992/small-test-repo
		- commits from master: 2
		- total commits in the repo: 2
	- forked repo: github.com/kuldeep-singh-blueoptima/small-test-repo
		- commits from master: 2
		- total commits in the repo: 2
	- config file:
	---
	[core]
		bare = true
	[remote "016b92d2-5b60-cbf8-a7d8-f0e0c6832d91"]
		url = git://github.com/rtyley/small-test-repo.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b60-cbf8-a7d8-f0e0c6832d91/*
	[remote "016b92d2-5b68-4376-da62-9bd3f44ccdf7"]
		url = git://github.com/kuldeep-singh-blueoptima/small-test-repo.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b68-4376-da62-9bd3f44ccdf7/*
	[remote "016b92d2-5b62-e877-df42-887c21e354bd"]
		url = git://github.com/kuldeep992/small-test-repo.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b62-e877-df42-887c21e354bd/*
	---

	f2cee90acf3c6644d51a37057845b98ab1580932.siva
	- repo: github.com/jtoy/awesome-tensorflow
		- commits from master: 263
		- total commits in the repo: 368
	- forked repo: github.com/SiweiLuo/awesome-tensorflow commits: 257
		- commits from master: 257
		- total commits in the repo: 257
	- forked repo: github.com/youtang1993/awesome-tensorflow commits: 257
		- commits from master: 257
		- total commits in the repo: 257
	- config file:
	---
	[core]
		bare = true
	[remote "016b92d2-5b5c-8dac-2ae6-6437e11dad17"]
		url = git://github.com/SiweiLuo/awesome-tensorflow.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b5c-8dac-2ae6-6437e11dad17/*
	[remote "016b92d2-5b5e-2925-a091-7cdb57ab3c5e"]
		url = git://github.com/youtang1993/awesome-tensorflow.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b5e-2925-a091-7cdb57ab3c5e/*
	[remote "016b92d2-5b58-9c19-84e5-ec45469a57ec"]
		url = git://github.com/jtoy/awesome-tensorflow.git
		isfork = false
		fetch = +refs/heads/*:refs/remotes/016b92d2-5b58-9c19-84e5-ec45469a57ec/*
	---

*/

const testDir = "_testdata"

func setupLibrary(
	t *testing.T,
	id string,
	opts *LibraryOptions,
) *Library {
	t.Helper()
	var require = require.New(t)

	fs, _ := setupMemFS(t, opts.Bucket)
	lib, err := NewLibrary(id, fs, opts)
	require.NoError(err)

	return lib
}

func setupMemFS(t *testing.T, bucket int) (billy.Filesystem, []string) {
	t.Helper()
	require := require.New(t)

	entries, err := ioutil.ReadDir(testDir)
	require.NoError(err)

	var sivas []string
	for _, e := range entries {
		if e.Mode().IsRegular() &&
			strings.HasSuffix(e.Name(), ".siva") {
			sivas = append(sivas, e.Name())
		}
	}

	require.True(len(sivas) > 0,
		"siva files not found in test directory")

	fs := memfs.New()

	for _, testSiva := range sivas {
		path := filepath.Join(testDir, testSiva)
		data, err := ioutil.ReadFile(path)
		require.NoError(err)

		id := toLocID(testSiva)
		siva := buildSivaPath(id, bucket)
		err = util.WriteFile(fs, siva, data, 0666)
		require.NoError(err)
	}

	return fs, sivas
}
