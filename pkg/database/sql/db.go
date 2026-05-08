package sql

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Connection interface {
	Queryer
	Execer
	Begin(ctx context.Context) (Transaction, error)
	TransactionContext(ctx context.Context, fn func(ctx context.Context) error) error
	Ping() error
	Close() error
}

type Transaction interface {
	Queryer
	Execer
	Commit() error
	Rollback() error
}

type Queryer interface {
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type Execer interface {
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
