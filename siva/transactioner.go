package siva

import (
	"time"

	errors "gopkg.in/src-d/go-errors.v1"
)

// ErrTransactionTimeout is returned when a repository can't be retrieved in
// transactional mode because of a timeout.
var ErrTransactionTimeout = errors.NewKind("timeout exceeded: unable to " +
	"retrieve repository from location %s in transactional mode.")

// transactioner manages synchronization to allow transactions on a Location.
type transactioner struct {
	notification chan struct{}
	timeout      time.Duration
	loc          *Location
	locReg       *locationRegistry
}

func newTransactioner(
	loc *Location,
	locReg *locationRegistry,
	timeout time.Duration,
) *transactioner {
	n := make(chan struct{}, 1)
	n <- struct{}{}
	return &transactioner{
		notification: n,
		timeout:      timeout,
		loc:          loc,
		locReg:       locReg,
	}
}

// Start requests permission for a new transaction. If it can't get it after a
// certain amount of time it will fail with an ErrTransactionTimeout error.
func (t *transactioner) Start() error {
	select {
	case <-t.notification:
		t.locReg.StartTransaction(t.loc)
		return nil
	case <-time.After(t.timeout):
		return ErrTransactionTimeout.New(t.loc.ID())
	}
}

// Stop signals the transaction is finished.
func (t *transactioner) Stop() {
	t.locReg.EndTransaction(t.loc)
	t.notification <- struct{}{}
}
