//go:build integration

package sql_test

import (
    "context"
    stdSql "database/sql"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/database/sql"
    "github.com/stretchr/testify/assert"
)

func TestSuccessfulDatabaseConnection(t *testing.T) {
    if !isContainerRunning() {
        postgresInit()
    }

    cfg := sql.Config{
        Host:     "localhost",
        Port:     pgsql.GetPort("5432/tcp"),
        User:     "postgres",
        Password: "test",
        Database: "test",
        SSLMode:  "disable",
    }
    db, err := sql.New(cfg)
    assert.NoError(t, db.Ping())
    assert.NoError(t, err)
    db.Close()
    assert.Error(t, db.Ping())
}

func TestUnsuccessfulConnectToToDB(t *testing.T) {
    cfg := sql.Config{
        Host:     "localhost",
        Port:     "2345",
        User:     "postgres",
        Password: "test",
        Database: "test",
        SSLMode:  "disable",
    }
    _, err := sql.New(cfg)
    assert.ErrorIs(t, err, sql.ErrConn)
    assert.Error(t, err)
}

func TestDbConn_Exec(t *testing.T) {
    db := db()
    tests := []struct {
        name    string
        args    []any
        query   string
        wantErr assert.ErrorAssertionFunc
    }{
        {
            name:  "Create affiliates table",
            query: `CREATE TABLE affiliates ( id SERIAL PRIMARY KEY, name VARCHAR );`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Create deals table",
            query: `CREATE TABLE deals (id SERIAL PRIMARY KEY NOT NULL, value int, affiliate_id INT UNIQUE NOT NULL REFERENCES affiliates (id) ON DELETE CASCADE)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Insert into table",
            query: `INSERT INTO affiliates (name) VALUES ('Jon Doe')`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Insert into deal",
            query: `INSERT INTO deals (affiliate_id, value) VALUES (1, 100)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Err while inserting duplicate value",
            query: `INSERT INTO deals (affiliate_id, value) VALUES (1, 100)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorIs(t, err, sql.ErrUniqueViolation)
            },
        },
        {
            name:  "Err while inserting value with invalid FK",
            query: `INSERT INTO deals (affiliate_id, value) VALUES (2, 100)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorIs(t, err, sql.ErrForeignKeyViolation)
            },
        },
        {
            name:  "Missing parameter Err",
            query: `INSERT INTO deals (affiliate_id, value) VALUES ($1, $2)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorIs(t, err, sql.ErrNoParameter)
            },
        },
        {
            name:  "Unmapped Err",
            query: `INSERT INTO deals (affiliate_id, value) VALUES (1, 'abd')`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.Error(t, err)
            },
        },
        {
            name:  "Insert multiple values with args",
            args:  []any{"Connor McGregor", "John Jones"},
            query: `INSERT INTO affiliates (name) VALUES ($1), ($2)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Syntax Err on insert",
            query: `INSERT INTO affiliates (name) VALUES (Jon Doe)`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorIs(t, err, sql.ErrSyntax)
            },
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            _, err := db.Exec(context.TODO(), tt.query, tt.args...)
            if !tt.wantErr(t, err) {
                return
            }
        })
    }
}

func TestDbConn_Get(t *testing.T) {
    db := db()
    mockMigration(db)
    type mockAffiliate struct {
        Id   int
        Name string
    }
    tests := []struct {
        name    string
        dest    any
        query   string
        expect  any
        wantErr assert.ErrorAssertionFunc
    }{
        {
            name:  "Get affiliate",
            dest:  &mockAffiliate{},
            query: `SELECT id, name FROM affiliates`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
            expect: &mockAffiliate{Id: 1, Name: "Jon Doe"},
        },
        {
            name:  "Not found",
            dest:  &mockAffiliate{},
            query: `SELECT id, name FROM affiliates WHERE name='John Rambo'`,
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorIs(t, err, stdSql.ErrNoRows)
            },
            expect: &mockAffiliate{},
        },
        {
            name:  "Unable to Scan the results from query, when a pointer is not passed",
            query: `SELECT id, Name FROM affiliates`,
            dest:  mockAffiliate{},
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.ErrorContains(t, err, "must pass a pointer, not a value, to StructScan destination")
            },
            expect: mockAffiliate{},
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := db.Get(context.TODO(), tt.dest, tt.query)
            if !tt.wantErr(t, err) {
                return
            }
            assert.Equal(t, tt.expect, tt.dest)
        })
    }
}

func TestDbConn_Select(t *testing.T) {
    db := db()
    mockMigration(db)
    type mockAffiliate struct {
        Id   int
        Name string
    }

    tests := []struct {
        name    string
        dest    any
        query   string
        args    []any
        expect  any
        wantErr assert.ErrorAssertionFunc
    }{
        {
            name:  "Select affiliates",
            dest:  &[]mockAffiliate{},
            query: "SELECT id, name FROM affiliates",
            args:  nil,
            expect: &[]mockAffiliate{
                {1, "Jon Doe"},
                {2, "Connor McGregor"},
                {3, "John Jones"},
            },
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:  "Select affiliates with WHERE clause",
            dest:  &[]mockAffiliate{},
            query: "SELECT id, name FROM affiliates WHERE name = $1",
            args:  []any{"Jon Doe"},
            expect: &[]mockAffiliate{
                {1, "Jon Doe"},
            },
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.NoError(t, err)
            },
        },
        {
            name:   "Invalid query",
            dest:   &[]mockAffiliate{},
            query:  "SELECT id name FROM affiliate WHERE name = $1",
            args:   []any{"John Doe"},
            expect: &[]mockAffiliate{},
            wantErr: func(t assert.TestingT, err error, i ...any) bool {
                return assert.Error(t, err)
            },
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := db.Select(context.TODO(), tt.dest, tt.query, tt.args...)
            if !tt.wantErr(t, err) {
                t.Log(tt.dest)
                return
            }
            assert.Equal(t, tt.expect, tt.dest)
        })
    }
}
