package sql_test

import (
	"context"
	"fmt"

	dbsql "github.com/diegodesousas/go-devkit/pkg/database/sql"
	"github.com/pkg/errors"
)

type account struct {
	ID      int    `db:"id"`
	Owner   string `db:"owner"`
	Balance int    `db:"balance"`
}

// connect is the shared set-up for the examples below. Every one of them needs a
// live PostgreSQL server, so they are compiled but not run.
func connect() dbsql.Connection {
	conn, err := dbsql.New(dbsql.Config{
		Host:        "localhost",
		Port:        "5432",
		User:        "postgres",
		Password:    "secret",
		Database:    "bank",
		SSLMode:     "disable",
		MaxOpenConn: 10,
		MaxIdleConn: 5,
	})
	if err != nil {
		panic(err)
	}

	return conn
}

func ExampleNew() {
	conn := connect()
	defer func() { _ = conn.Close() }()

	ctx := context.Background()

	var a account
	if err := conn.Get(ctx, &a, "SELECT id, owner, balance FROM accounts WHERE id = $1", 1); err != nil {
		panic(err)
	}
	fmt.Println(a.Owner, a.Balance)

	var all []account
	if err := conn.Select(ctx, &all, "SELECT id, owner, balance FROM accounts"); err != nil {
		panic(err)
	}
	fmt.Println(len(all))
}

// The transaction lives in the context, so the statements inside fn call the
// same Connection methods they would outside one. Returning an error rolls back;
// returning nil commits.
func ExampleConnection_TransactionContext() {
	conn := connect()
	defer func() { _ = conn.Close() }()

	err := conn.TransactionContext(context.Background(), func(ctx context.Context) error {
		if _, err := conn.Exec(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", 100, 1); err != nil {
			return err
		}

		_, err := conn.Exec(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", 100, 2)
		return err
	})
	if err != nil {
		panic(err)
	}
}

// PostgreSQL error codes are translated into the sentinels declared by this
// package, so a caller branches with errors.Is instead of matching on strings.
func ExampleErrUniqueViolation() {
	conn := connect()
	defer func() { _ = conn.Close() }()

	_, err := conn.Exec(context.Background(), "INSERT INTO accounts (id, owner) VALUES ($1, $2)", 1, "ana")

	switch {
	case errors.Is(err, dbsql.ErrUniqueViolation):
		fmt.Println("account already exists")
	case errors.Is(err, dbsql.ErrForeignKeyViolation):
		fmt.Println("owner does not exist")
	case err != nil:
		fmt.Println("insert failed:", err)
	}
}
