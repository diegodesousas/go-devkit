// Package upsert builds PostgreSQL INSERT ... ON CONFLICT statements.
//
// Build assembles the statement from options and returns it with its argument
// list, ready for the Exec method of a sql.Connection:
//
//	query, args, err := upsert.Build(
//		upsert.WithTable("orders"),
//		upsert.WithConstraints("id"),
//		upsert.WithInsertValues(upsert.ColumnMap{"id": id, "total": total}),
//		upsert.WithOnConflictUpdate(upsert.ColumnMap{"total": total}),
//	)
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.Exec(ctx, query, args...)
//
// The conflict action is either an update or a no-op: WithOnConflictUpdate
// writes DO UPDATE SET, WithOnConflictDoNothing writes DO NOTHING, and asking
// for both returns ErrIncompatibleOptions. Either one needs at least one
// constraint column, since that is what names the conflict target.
//
// Unlike the option sets elsewhere in this repository, the options here are
// validated - but at Build time, not as they are applied, so the error names the
// combination rather than the individual call.
//
// Placeholders are PostgreSQL-style ($1, $2, ...). The builder is squirrel.
package upsert
