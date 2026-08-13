package sql

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type tx struct {
	tx *sqlx.Tx
}

func (t *tx) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if err := t.tx.GetContext(ctx, dest, query, args...); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (t *tx) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if err := t.tx.SelectContext(ctx, dest, query, args...); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (t *tx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
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

func WithTransaction(ctx context.Context, transaction Transaction) context.Context {
	return context.WithValue(ctx, txKey, transaction)
}
