package sql

import (
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
)

// Errors that can be returned by the sql package.
var (

	// ErrConn represents an error while connecting to the database.
	ErrConn = errors.New("error while connecting to database")

	// ErrSyntax represents a syntax error with code 42601, in which the query is malformed.
	ErrSyntax = errors.New("syntax Error 42601")

	// ErrUniqueViolation represents a unique violation error with code 23505, in which the value
	// already exists in the database and can't be duplicated.
	ErrUniqueViolation = errors.New("unique violation 23505")

	// ErrForeignKeyViolation represents a foreign key violation error with code 23503, in which the FK does
	// not exist.
	ErrForeignKeyViolation = errors.New("foreign key violation 23503")

	// ErrNoParameter represents a no parameter error with code 42P02, that may happen when a parameter is missing
	// for example: 'INSERT INTO deals (affiliate_id, value) VALUES ($1, $2)', where $1 is missing.
	ErrNoParameter = errors.New("no parameter 42P02")

	// GeneralErr represents any err that has not been mapped yet, the err then should be asserted to
	// the pgconn.PgError type.
	GeneralErr = errors.New("something went wrong")
)

func pgErrWrapper(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return errors.WithStack(err)
	}

	switch pgErr.Code {
	case "42601":
		return errors.Wrap(ErrSyntax, pgErr.Message)
	case "23505":
		return errors.Wrap(ErrUniqueViolation, pgErr.Message)
	case "23503":
		return errors.Wrap(ErrForeignKeyViolation, pgErr.Message)
	case "42P02":
		return errors.Wrap(ErrNoParameter, pgErr.Message)
	default:
		return errors.Wrap(GeneralErr, err.Error())
	}
}
