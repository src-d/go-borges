package legacysiva_test

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/legacysiva"

	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func Example() {
	headReg := regexp.MustCompile(`^refs/heads/HEAD/([0-9a-f-]{36})$`)

	fs := osfs.New("./_testdata")
	lib, err := legacysiva.NewLibrary("library", fs, &legacysiva.LibraryOptions{
		Bucket: 0,
	})
	if err != nil {
		panic(err)
	}

	repos, err := lib.Repositories(borges.ReadOnlyMode)
	if err != nil {
		panic(err)
	}

	var heads []string
	err = repos.ForEach(func(r borges.Repository) error {
		refs, err := r.R().References()
		if err != nil {
			panic(err)
		}

		err = refs.ForEach(func(ref *plumbing.Reference) error {
			refName := ref.Name().String()
			if !headReg.Match([]byte(refName)) {
				return nil
			}

			location := r.Location().ID()
			id := headReg.FindAllString(refName, -1)[0]
			head := ref.Hash().String()

			h := fmt.Sprintf("location: %v, id %v, head: %v", location, id, head)
			heads = append(heads, h)
			return nil
		})

		return err
	})

	if err != nil {
		panic(err)
	}

	sort.Strings(heads)
	for _, h := range heads {
		fmt.Println(h)
	}

	// Output:
	// location: 3974996807a9f596cf25ac3a714995c24bb97e2c, id refs/heads/HEAD/016b92d2-5b60-cbf8-a7d8-f0e0c6832d91, head: ce1e0703402e989bedf03d5df535401340f54b42
	// location: 3974996807a9f596cf25ac3a714995c24bb97e2c, id refs/heads/HEAD/016b92d2-5b62-e877-df42-887c21e354bd, head: ce1e0703402e989bedf03d5df535401340f54b42
	// location: 3974996807a9f596cf25ac3a714995c24bb97e2c, id refs/heads/HEAD/016b92d2-5b68-4376-da62-9bd3f44ccdf7, head: ce1e0703402e989bedf03d5df535401340f54b42
	// location: f2cee90acf3c6644d51a37057845b98ab1580932, id refs/heads/HEAD/016b92d2-5b58-9c19-84e5-ec45469a57ec, head: 4de1a2d995bc79d6e39bef647accbde6bec9093f
	// location: f2cee90acf3c6644d51a37057845b98ab1580932, id refs/heads/HEAD/016b92d2-5b5c-8dac-2ae6-6437e11dad17, head: 38aec3212d377ae36b72fcc068f57e7a6344c5d4
	// location: f2cee90acf3c6644d51a37057845b98ab1580932, id refs/heads/HEAD/016b92d2-5b5e-2925-a091-7cdb57ab3c5e, head: 38aec3212d377ae36b72fcc068f57e7a6344c5d4
}
