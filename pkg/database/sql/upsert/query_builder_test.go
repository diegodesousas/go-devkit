package upsert_test

import (
    "fmt"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/database/sql/upsert"
    "github.com/stretchr/testify/assert"
)

func TestBuild(t *testing.T) {
    type params struct {
        options []upsert.Option
    }

    type expected struct {
        query string
        args  []any
        err   error
    }

    tests := []struct {
        name     string
        params   params
        expected expected
    }{
        {
            name: "Success simple insert",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                },
            },
            expected: expected{
                query: "INSERT INTO bets (amount,id,status) VALUES ($1,$2,$3)",
                args:  []any{100, "1234", "finished"},
                err:   nil,
            },
        },
        {
            name: "Success with on conflict update",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictUpdate(upsert.ColumnMap{
                        "status": "finished",
                    }),
                },
            },
            expected: expected{
                query: "INSERT INTO bets (amount,id,status) VALUES ($1,$2,$3) ON CONFLICT(id) DO UPDATE   SET status = $4",
                args:  []any{100, "1234", "finished", "finished"},
                err:   nil,
            },
        },
        {
            name: "Success with on conflict update with more than one constraint",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id, status"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictUpdate(upsert.ColumnMap{
                        "status": "finished",
                    }),
                },
            },
            expected: expected{
                query: "INSERT INTO bets (amount,id,status) VALUES ($1,$2,$3) ON CONFLICT(id, status) DO UPDATE   SET status = $4",
                args:  []any{100, "1234", "finished", "finished"},
                err:   nil,
            },
        },
        {
            name: "Success with on conflict do nothing",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictDoNothing(),
                },
            },
            expected: expected{
                query: "INSERT INTO bets (amount,id,status) VALUES ($1,$2,$3) ON CONFLICT(id) DO NOTHING",
                args:  []any{100, "1234", "finished"},
                err:   nil,
            },
        },
        {
            name: "Error without table name option",
            params: params{
                options: []upsert.Option{
                    upsert.WithConstraints("id"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictDoNothing(),
                },
            },
            expected: expected{
                query: "",
                args:  []any(nil),
                err:   fmt.Errorf("insert statements must specify a table"),
            },
        },
        {
            name: "Error without constraint option",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictDoNothing(),
                },
            },
            expected: expected{
                query: "",
                args:  []any(nil),
                err:   upsert.ErrInvalidConstraint,
            },
        },
        {
            name: "Error without insert values option",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id"),
                    upsert.WithOnConflictDoNothing(),
                },
            },
            expected: expected{
                query: "",
                args:  []any(nil),
                err:   upsert.ErrInsertValuesIsEmpty,
            },
        },
        {
            name: "Error with empty update values option",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictUpdate(upsert.ColumnMap{}),
                },
            },
            expected: expected{
                query: "",
                args:  []any(nil),
                err:   upsert.ErrUpdateValuesIsEmpty,
            },
        },
        {
            name: "Error with incompatible options",
            params: params{
                options: []upsert.Option{
                    upsert.WithTable("bets"),
                    upsert.WithConstraints("id"),
                    upsert.WithInsertValues(upsert.ColumnMap{
                        "id":     "1234",
                        "amount": 100,
                        "status": "finished",
                    }),
                    upsert.WithOnConflictUpdate(upsert.ColumnMap{
                        "status": "finished",
                    }),
                    upsert.WithOnConflictDoNothing(),
                },
            },
            expected: expected{
                query: "",
                args:  []any(nil),
                err:   upsert.ErrIncompatibleOptions,
            },
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            query, args, err := upsert.Build(tt.params.options...)

            assert.Equal(t, tt.expected.err, err)
            assert.Equal(t, tt.expected.query, query)
            assert.Equal(t, tt.expected.args, args)
        })
    }
}
