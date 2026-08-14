package sql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type tx struct {
	tx *sqlx.Tx
}

func (t *tx) Get(ctx context.Context, dest any, query string, args ...any) error {
	if err := t.tx.GetContext(ctx, dest, query, args...); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (t *tx) Select(ctx context.Context, dest any, query string, args ...any) error {
	if err := t.tx.SelectContext(ctx, dest, query, args...); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (t *tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, pgErrWrapper(err)
	}

	return res, nil
}

func (t *tx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (t *tx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

// WithTransaction returns a context carrying transaction, so that later calls
// to Get, Select and Exec on a Connection run inside it.
//
// TransactionContext does this for you. Call it directly only when managing a
// transaction by hand.
func WithTransaction(ctx context.Context, transaction Transaction) context.Context {
	return context.WithValue(ctx, txKey, transaction)
}
