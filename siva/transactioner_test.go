package siva

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupTranstactioner(t *testing.T, to time.Duration) *transactioner {
	t.Helper()

	var require = require.New(t)

	loc := &Location{
		id:   "foo",
		path: "foo.siva",
	}

	locReg, err := newLocationRegistry(1)
	require.NoError(err)
	locReg.Add(loc)
	lr, ok := locReg.Get(loc.ID())
	require.True(ok)
	require.Equal(loc, lr)

	txer := newTransactioner(loc, locReg, to)
	require.NotNil(txer)
	require.Equal(loc, txer.loc)
	require.Equal(locReg, txer.locReg)

	return txer
}

func TestTransactioner(t *testing.T) {
	var require = require.New(t)

	txer := setupTranstactioner(t, txTimeout)
	const transactions = 1000
	var (
		w     sync.WaitGroup
		count int
	)

	for i := 0; i < transactions; i++ {
		w.Add(1)
		go func() {
			require.NoError(txer.Start())
			count++
			txer.Stop()
			w.Done()
		}()
	}
	w.Wait()
	require.Equal(transactions, count)
}

func TestTransactioner_Timeout(t *testing.T) {
	var require = require.New(t)

	txer := setupTranstactioner(t, 100*time.Millisecond)

	err := txer.Start()
	require.NoError(err)

	err = txer.Start()
	require.EqualError(err,
		ErrTransactionTimeout.New(txer.loc.ID()).Error())

	txer.Stop()
}
