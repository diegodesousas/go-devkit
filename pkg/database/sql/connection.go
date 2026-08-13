package sql

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type key string

const txKey key = "tx"

type Config struct {
	Host            string
	Port            string
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxOpenConn     int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
	MaxIdleConn     int
}

// redactedPassword is what String reports in place of a non-empty password.
const redactedPassword = "****"

// dsn builds the connection string handed to the driver. It carries the
// password in clear text, so its result must never be logged nor embedded in
// an error message. Use String for anything human-facing.
func (cfg Config) dsn() string {
	return cfg.format(cfg.Password)
}

// String implements fmt.Stringer with the password redacted. Config is a
// plausible thing to log, and without this method every %v on a Config - or on
// any struct holding one - would print the credential in clear text.
func (cfg Config) String() string {
	password := cfg.Password
	if password != "" {
		password = redactedPassword
	}

	return cfg.format(password)
}

func (cfg Config) format(password string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, password, cfg.Database, cfg.SSLMode)
}

// dbConn represents a new database connection
type dbConn struct {
	db *sqlx.DB
}

func New(cfg Config) (Connection, error) {
	db, err := sqlx.Connect("pgx", cfg.dsn())
	if err != nil {
		return nil, errors.Wrapf(ErrConn, "%s", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConn)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetMaxIdleConns(cfg.MaxIdleConn)

	return &dbConn{db: db}, nil
}

func (c *dbConn) txFromContext(ctx context.Context) (Transaction, bool) {
	tx, ok := ctx.Value(txKey).(*tx)
	return tx, ok
}

func (c *dbConn) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "database.get", tracer.SpanType("db"))
	defer span.Finish()

	span.SetTag("query", query)

	if tx, ok := c.txFromContext(ctx); ok {
		return pgErrWrapper(tx.Get(ctx, dest, query, args...))
	}

	return pgErrWrapper(c.db.GetContext(ctx, dest, query, args...))
}

func (c *dbConn) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "database.select", tracer.SpanType("db"))
	defer span.Finish()

	span.SetTag("query", query)

	if tx, ok := c.txFromContext(ctx); ok {
		return pgErrWrapper(tx.Select(ctx, dest, query, args...))
	}

	if err := c.db.SelectContext(ctx, dest, query, args...); err != nil {
		return pgErrWrapper(err)
	}

	return nil
}

func (c *dbConn) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "database.exec", tracer.SpanType("db"))
	defer span.Finish()

	span.SetTag("query", query)

	if tx, ok := c.txFromContext(ctx); ok {
		return tx.Exec(ctx, query, args...)
	}

	res, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, pgErrWrapper(err)
	}

	return res, nil
}

func (c *dbConn) Begin(ctx context.Context) (Transaction, error) {
	txCtx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &tx{tx: txCtx}, nil
}

func (c *dbConn) Ping() error {
	return pgErrWrapper(c.db.Ping())
}

func (c *dbConn) Close() error {
	return pgErrWrapper(c.db.Close())
}

func (c *dbConn) TransactionContext(ctx context.Context, fn func(ctx context.Context) error) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "database.transaction.begin", tracer.SpanType("db"))
	defer span.Finish()

	trx, err := c.Begin(ctx)
	if err != nil {
		return err
	}

	ctx = WithTransaction(ctx, trx)

	if fnErr := fn(ctx); fnErr != nil {
		span.SetTag("final.state", "rollback")
		if err := trx.Rollback(); err != nil {
			return errors.Cause(stderrors.Join(fnErr, err))
		}

		return fnErr
	}

	if err := trx.Commit(); err != nil {
		span.SetTag("final.state", "commit")
		return err
	}

	return nil
}
