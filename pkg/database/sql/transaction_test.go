//go:build integration

package sql_test

import (
	"context"
	stdSql "database/sql"
	"testing"

	"github.com/pkg/errors"

	"github.com/diegodesousas/go-devkit/pkg/database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuccessfulTransaction(t *testing.T) {
	db := db()
	mockMigration(db)
	type mockDeal struct {
		Id    int
		Value int
	}

	updateQuery := `UPDATE deals SET value = $1 WHERE deals.affiliate_id = $2`
	getQuery := `SELECT id, value FROM deals WHERE affiliate_id = $1`

	ctx := context.TODO()
	tx, err := db.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, updateQuery, 110, 1)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, updateQuery, 120, 1)
	require.NoError(t, err)

	gotDeal := &mockDeal{}

	err = tx.Commit()
	assert.NoError(t, err)

	err = db.Get(ctx, gotDeal, getQuery, 1)
	require.NoError(t, err)

	expectedDeal := &mockDeal{1, 120}
	assert.EqualValues(t, expectedDeal, gotDeal)
}

func TestTransactionContext_Fn_Success(t *testing.T) {
	conn := db()
	mockMigration(conn)

	type mockDeal struct {
		Id    int
		Value int
	}

	updateQuery := `UPDATE deals SET value = $1 WHERE deals.affiliate_id = $2`
	getQuery := `SELECT id, value FROM deals WHERE affiliate_id = $1`

	err := conn.TransactionContext(context.Background(), func(ctx context.Context) error {

		gotDeal := mockDeal{}
		err := conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal := mockDeal{1, 100}
		assert.EqualValues(t, expectedDeal, gotDeal)

		_, err = conn.Exec(ctx, updateQuery, 150, 1)
		assert.Nil(t, err)

		gotDeal = mockDeal{}
		err = conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal = mockDeal{1, 150}
		assert.Equal(t, expectedDeal, gotDeal)

		return nil
	})

	assert.Nil(t, err)
}

func TestTransactionContext_Fn_Error(t *testing.T) {
	conn := db()
	mockMigration(conn)

	type mockDeal struct {
		Id    int
		Value int
	}

	updateQuery := `UPDATE deals SET value = $1 WHERE deals.affiliate_id = $2`
	getQuery := `SELECT id, value FROM deals WHERE affiliate_id = $1`

	err := conn.TransactionContext(context.Background(), func(ctx context.Context) error {
		gotDeal := mockDeal{}
		err := conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal := mockDeal{1, 100}
		assert.EqualValues(t, expectedDeal, gotDeal)

		_, err = conn.Exec(ctx, updateQuery, 150, 1)
		assert.Nil(t, err)

		gotDeal = mockDeal{}
		err = conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal = mockDeal{1, 150}
		assert.Equal(t, expectedDeal, gotDeal)

		return errors.New("unexpected error")
	})

	assert.EqualError(t, err, "unexpected error")

	gotDeal := mockDeal{}
	err = conn.Get(context.Background(), &gotDeal, getQuery, 1)
	assert.Nil(t, err)

	expectedDeal := mockDeal{1, 100}
	assert.EqualValues(t, expectedDeal, gotDeal)
}

func TestTransactionContext_Begin_Error(t *testing.T) {
	conn := db()
	mockMigration(conn)

	err := conn.Close()
	assert.Nil(t, err)

	err = conn.TransactionContext(context.Background(), func(ctx context.Context) error {
		return nil
	})

	assert.Error(t, err)
}

func TestTransactionContext_Rollback_Error(t *testing.T) {
	conn := db()
	mockMigration(conn)

	err := conn.TransactionContext(context.Background(), func(ctx context.Context) error {

		err := conn.Close()
		assert.Nil(t, err)

		return errors.New("fn error")
	})

	assert.Error(t, err)
}

func TestTransactionContext_Commit_Error(t *testing.T) {
	conn := db()
	mockMigration(conn)

	type mockDeal struct {
		Id    int
		Value int
	}

	updateQuery := `UPDATE deals SET value = $1 WHERE deals.affiliate_id = $2`
	getQuery := `SELECT id, value FROM deals WHERE affiliate_id = $1`

	err := conn.TransactionContext(context.Background(), func(ctx context.Context) error {
		gotDeal := mockDeal{}
		err := conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal := mockDeal{1, 100}
		assert.EqualValues(t, expectedDeal, gotDeal)

		_, err = conn.Exec(ctx, updateQuery, 150, 1)
		assert.Nil(t, err)

		gotDeal = mockDeal{}
		err = conn.Get(ctx, &gotDeal, getQuery, 1)
		assert.Nil(t, err)

		expectedDeal = mockDeal{1, 150}
		assert.Equal(t, expectedDeal, gotDeal)

		err = pgsql.Close()
		assert.Nil(t, err)

		return nil
	})

	assert.Error(t, err)
}

func TestSuccessfulRollback(t *testing.T) {
	db := db()
	mockMigration(db)
	type mockDeal struct {
		Id    int
		Value int
	}

	updateQuery := `UPDATE deals SET value = $1 WHERE deals.affiliate_id = $2`
	getQuery := `SELECT id, value FROM deals WHERE affiliate_id = $1`

	ctx := context.TODO()
	tx, err := db.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, updateQuery, 90, 1)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, updateQuery, 70, 1)
	require.NoError(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	gotDeal := &mockDeal{}
	err = db.Get(ctx, gotDeal, getQuery, 1)
	require.NoError(t, err)

	expectedDeal := &mockDeal{1, 100}
	assert.EqualValues(t, expectedDeal, gotDeal)
}

func TestErrOnRollback(t *testing.T) {
	db := db()
	query := `CREATE TABLE IF NOT EXISTS test (id int)`

	ctx := context.TODO()
	tx, err := db.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, query)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	err = tx.Rollback()
	assert.ErrorIs(t, err, stdSql.ErrTxDone)
	assert.Error(t, err)

}

func TestTransactionFromContext(t *testing.T) {
	db := db()
	mockMigration(db)
	type mockAffiliate struct {
		Name string
	}

	ctx := context.Background()
	newTx, err := db.Begin(ctx)
	require.NoError(t, err)
	ctx = sql.WithTransaction(ctx, newTx)
	query := `INSERT INTO affiliates (name) VALUES ('New Affiliate')`
	_, err = db.Exec(ctx, query)
	require.NoError(t, err)

	got := &mockAffiliate{}
	expected := &mockAffiliate{Name: "New Affiliate"}
	err = newTx.Get(ctx, got, `SELECT name FROM affiliates WHERE name = 'New Affiliate'`)
	require.NoError(t, err)
	require.Equal(t, expected, got)

	gotSelect := &[]mockAffiliate{}
	expectedSelect := &[]mockAffiliate{{Name: "New Affiliate"}}
	err = newTx.Select(ctx, gotSelect, `SELECT name FROM affiliates WHERE name = 'New Affiliate'`)
	require.NoError(t, err)
	assert.Equal(t, expectedSelect, gotSelect)

	err = newTx.Commit()
	require.NoError(t, err)
}

func TestTransactionBeginErr(t *testing.T) {
	db := db()
	db.Close()
	_, err := db.Begin(context.TODO())
	assert.Error(t, err)
}

func Test_tx_Exec(t *testing.T) {
	db := db()
	tests := []struct {
		name      string
		query     string
		wantErr   assert.ErrorAssertionFunc
		commitErr assert.ErrorAssertionFunc
		want      any
	}{
		{
			name:  "Test Exec()",
			query: `CREATE TABLE affiliates ( id SERIAL PRIMARY KEY, name VARCHAR );`,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
			commitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err)
			},
		},
		{
			name:  "Test failed Exec()",
			query: `INSERT INTO deals (affiliate_id, value) VALUES (1, 100)`,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.Begin(context.TODO())
			require.NoError(t, err)
			_, err = tx.Exec(context.TODO(), tt.query)
			if !tt.wantErr(t, err, tt.name) {
				return
			}
			err = tx.Commit()
			if !tt.wantErr(t, err, tt.name) {
				return
			}
		})
	}
}

func Test_tx_Get(t *testing.T) {
	db := db()
	mockMigration(db)
	type mockAffiliate struct {
		Id   int
		Name string
	}
	tests := []struct {
		name          string
		dest          any
		query         string
		expect        any
		wantErr       assert.ErrorAssertionFunc
		wantCommitErr assert.ErrorAssertionFunc
	}{
		{
			name:  "Get affiliate",
			dest:  &mockAffiliate{},
			query: `SELECT id, name FROM affiliates`,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
			wantCommitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
			expect: &mockAffiliate{Id: 1, Name: "Jon Doe"},
		},
		{
			name:   "Not found",
			dest:   &mockAffiliate{},
			query:  `SELECT id, name FROM affiliates WHERE name='John Rambo'`,
			expect: &mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, stdSql.ErrNoRows)
			},
			wantCommitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name:   "Unable to Scan the results from query, when a pointer is not passed",
			query:  `SELECT id, Name FROM affiliates`,
			dest:   mockAffiliate{},
			expect: mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "must pass a pointer, not a value, to StructScan destination")
			},
			wantCommitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.Begin(context.TODO())
			require.NoError(t, err)
			err = tx.Get(context.TODO(), tt.dest, tt.query)
			if !tt.wantErr(t, err, tt.name) {
				return
			}
			err = tx.Commit()
			if !tt.wantCommitErr(t, err, tt.name) {
				return
			}
		})
	}
}

func Test_TransactionContext_Get(t *testing.T) {
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
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
			expect: &mockAffiliate{Id: 1, Name: "Jon Doe"},
		},
		{
			name:   "Not found",
			dest:   &mockAffiliate{},
			query:  `SELECT id, name FROM affiliates WHERE name='John Rambo'`,
			expect: &mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, stdSql.ErrNoRows)
			},
		},
		{
			name:   "Unable to Scan the results from query, when a pointer is not passed",
			query:  `SELECT id, Name FROM affiliates`,
			dest:   mockAffiliate{},
			expect: mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "must pass a pointer, not a value, to StructScan destination")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.TransactionContext(context.Background(), func(ctx context.Context) error {
				return db.Get(ctx, tt.dest, tt.query)
			})

			if !tt.wantErr(t, err, tt.name) {
				return
			}
		})
	}
}

func Test_tx_Select(t *testing.T) {
	db := db()
	mockMigration(db)
	type mockAffiliate struct {
		Id   int
		Name string
	}

	tests := []struct {
		name          string
		dest          any
		query         string
		args          []any
		expect        any
		wantErr       assert.ErrorAssertionFunc
		wantCommitErr assert.ErrorAssertionFunc
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
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
			wantCommitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name:   "Invalid query",
			dest:   &[]mockAffiliate{},
			query:  "SELECT id name FROM affiliate WHERE name = $1",
			args:   []any{"John Doe"},
			expect: &[]mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err)
			},
			wantCommitErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.Begin(context.TODO())
			require.NoError(t, err)
			err = tx.Select(context.TODO(), tt.dest, tt.query)
			if !tt.wantErr(t, err, tt.name) {
				return
			}
			err = tx.Commit()
			if !tt.wantCommitErr(t, err, tt.name) {
				return
			}
		})
	}
}

func Test_TransactionContext_Select(t *testing.T) {
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
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name:   "Invalid query",
			dest:   &[]mockAffiliate{},
			query:  "SELECT id name FROM affiliate WHERE name = $1",
			args:   []any{"John Doe"},
			expect: &[]mockAffiliate{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.TransactionContext(context.Background(), func(ctx context.Context) error {

				return db.Select(ctx, tt.dest, tt.query)
			})

			if !tt.wantErr(t, err, tt.name) {
				return
			}
		})
	}
}
