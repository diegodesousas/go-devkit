// Package sql wraps a PostgreSQL connection with context-scoped transactions,
// Datadog tracing and typed errors.
//
// A connection is built from a Config and used through the Connection
// interface:
//
//	conn, err := sql.New(sql.Config{
//		Host: "localhost", Port: "5432",
//		User: "postgres", Password: pw,
//		Database: "orders", SSLMode: "disable",
//		MaxOpenConn: 10, MaxIdleConn: 5,
//	})
//
//	var o Order
//	err = conn.Get(ctx, &o, "SELECT id, total FROM orders WHERE id = $1", id)
//
// Scanning is sqlx over the pgx stdlib driver: columns map to struct fields by
// lowercased field name, or by an explicit `db` tag, and Select fills a slice.
//
// Transactions travel in the context rather than in a separate handle.
// TransactionContext begins one, puts it in the context it passes to fn, and
// commits when fn returns nil or rolls back when it returns an error:
//
//	err := conn.TransactionContext(ctx, func(ctx context.Context) error {
//		if _, err := conn.Exec(ctx, debit, from); err != nil {
//			return err
//		}
//		_, err := conn.Exec(ctx, credit, to)
//		return err
//	})
//
// The point of that shape is that repository code keeps calling the same
// Connection methods either way: Get, Select and Exec look for a transaction in
// the context and use it when there is one, or go to the pool when there is
// not. Nothing has to be threaded through a separate parameter.
//
// Driver errors are translated. Known PostgreSQL SQLSTATE codes become the
// sentinel errors declared in this package, so callers match with errors.Is
// instead of comparing strings:
//
//	if errors.Is(err, sql.ErrUniqueViolation) { ... }
//
// Anything unmapped becomes ErrGeneral. Note that the translation keeps the
// message but not the *pgconn.PgError itself, so details such as the constraint
// name are not reachable from the returned error.
//
// Config carries a password. Its String method redacts it, but the dsn built
// for the driver does not - never log the value returned by an internal helper,
// and prefer %v on the Config, which goes through String.
//
// Every method on the connection opens a Datadog span tagged with the query.
package sql
