package upsert_test

import (
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/database/sql/upsert"
)

func ExampleBuild() {
	query, args, err := upsert.Build(
		upsert.WithTable("orders"),
		upsert.WithConstraints("id"),
		upsert.WithInsertValues(upsert.ColumnMap{"id": 1, "total": 250}),
		upsert.WithOnConflictUpdate(upsert.ColumnMap{"total": 250}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(query)
	fmt.Println(args...)

	// The run of spaces before SET is padding left by the underlying builder.
	// It is redundant whitespace, not a syntax error, and PostgreSQL ignores it.

	// Output:
	// INSERT INTO orders (id,total) VALUES ($1,$2) ON CONFLICT(id) DO UPDATE   SET total = $3
	// 1 250 250
}

// WithOnConflictDoNothing turns the statement into an insert-if-absent.
func ExampleBuild_doNothing() {
	query, args, err := upsert.Build(
		upsert.WithTable("orders"),
		upsert.WithConstraints("id"),
		upsert.WithInsertValues(upsert.ColumnMap{"id": 1}),
		upsert.WithOnConflictDoNothing(),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(query)
	fmt.Println(args...)

	// Output:
	// INSERT INTO orders (id) VALUES ($1) ON CONFLICT(id) DO NOTHING
	// 1
}

// Asking for both conflict actions is rejected at Build time.
func ExampleBuild_incompatibleOptions() {
	_, _, err := upsert.Build(
		upsert.WithTable("orders"),
		upsert.WithConstraints("id"),
		upsert.WithInsertValues(upsert.ColumnMap{"id": 1}),
		upsert.WithOnConflictDoNothing(),
		upsert.WithOnConflictUpdate(upsert.ColumnMap{"total": 250}),
	)

	fmt.Println(err)

	// Output:
	// incompatible options (WithOnConflictDoNothing, WithOnConflictUpdate)
}
