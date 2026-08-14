package sql

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Connection is a PostgreSQL connection pool.
//
// Get, Select and Exec look for a transaction in the context and use it when
// present, so the same call works inside and outside TransactionContext.
//
// Ping and Close take no context and cannot be cancelled.
type Connection interface {
	Queryer
	Execer
	Begin(ctx context.Context) (Transaction, error)
	TransactionContext(ctx context.Context, fn func(ctx context.Context) error) error
	Ping() error
	Close() error
}

// Transaction is an open database transaction. Prefer
// Connection.TransactionContext, which commits and rolls back for you; use this
// directly only when the transaction has to outlive a single function.
type Transaction interface {
	Queryer
	Execer
	Commit() error
	Rollback() error
}

// Queryer reads rows. Get scans a single row into dest and returns
// sql.ErrNoRows when there is none; Select scans every row into a slice.
type Queryer interface {
	Get(ctx context.Context, dest any, query string, args ...any) error
	Select(ctx context.Context, dest any, query string, args ...any) error
}

// Execer runs a statement that returns no rows.
type Execer interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
}
