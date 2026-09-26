// Package shutdown releases an application's resources in order when it stops.
//
// Graceful runs a list of steps - stop the HTTP server, then close the
// database, for instance - under a single deadline:
//
//	stopServer := server.Run()
//	<-interrupt
//
//	err := shutdown.Graceful(ctx, 15*time.Second,
//		shutdown.Step(stopServer),
//		func(context.Context) error { return conn.Close() },
//	)
//
// The deadline bounds the whole sequence, so a server that never finishes
// draining its connections cannot hang the process past the orchestrator's
// grace period. Keep the timeout below that period (30s by default on
// Kubernetes). A failing or timed-out step does not stop the sequence: the
// remaining steps still run and every error is joined into the returned one.
package shutdown
