//go:build integration

package sql_test

import (
    "context"
    "fmt"
    "log"
    "os"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/database/sql"
    "github.com/jmoiron/sqlx"
    "github.com/ory/dockertest/v3"
    "github.com/ory/dockertest/v3/docker"
)

// pool represents an instance of a connection to a Docker API
var pool *dockertest.Pool
var pgsql *dockertest.Resource

func TestMain(m *testing.M) {
    var err error
    pool, err = dockertest.NewPool("")
    if err != nil {
        log.Fatalf("Could not build pool: %s", err)
    }

    err = pool.Client.Ping()
    if err != nil {
        log.Fatalf("Could not ping docker: %s", err)
    }

    postgresInit()

    code := m.Run()

    err = pool.Purge(pgsql)
    if err != nil {
        fmt.Printf("Could not purge resource: %s", err)
    }

    os.Exit(code)
}

// postgresInit just initializes a postgres Image
func postgresInit() {
    resource, err := pool.RunWithOptions(&dockertest.RunOptions{
        Repository: "postgres",
        Tag:        "15.3",
        Env:        []string{"POSTGRES_PASSWORD=test", "POSTGRES_DB=test"},
    }, func(config *docker.HostConfig) {
        config.AutoRemove = true
        config.RestartPolicy = docker.RestartPolicy{
            Name: "no",
        }
    })
    if err != nil {
        log.Fatalf("unable to connect to docker %s", err)
    }

    err = resource.Expire(120)
    if err != nil {
        log.Fatalf("unable not expire resource: %s", err)
    }

    var db *sqlx.DB
    if err := pool.Retry(func() error {
        dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
            "localhost", resource.GetPort("5432/tcp"), "postgres", "test", "test", "disable")
        db, err = sqlx.Connect("pgx", dsn)
        if err != nil {
            return err
        }
        return db.Ping()
    }); err != nil {
        log.Fatalf("Could not connect to database: %s", err)
    }

    pgsql = resource
}

func db() sql.Connection {
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
    if err != nil {
        log.Fatalf("Could not connect to database: %s", err)
    }
    resetDatabase(db)
    return db
}

func isContainerRunning() bool {
    exitCode, _ := pgsql.Exec([]string{"echo", "up"}, dockertest.ExecOptions{})
    return exitCode == 0
}

func resetDatabase(db sql.Connection) {
    query := `
	DO $$ DECLARE
    r RECORD;
	BEGIN
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
					EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
			END LOOP;
	END $$;
	`

    _, err := db.Exec(context.Background(), query)
    if err != nil {
        panic(err)
    }
}

func mockMigration(db sql.Connection) {
    _, err := db.Exec(context.TODO(), `CREATE TABLE affiliates ( id SERIAL PRIMARY KEY, name VARCHAR );`)
    if err != nil {
        panic(err)
    }
    _, err = db.Exec(context.TODO(), `CREATE TABLE deals (id SERIAL PRIMARY KEY NOT NULL, value int, affiliate_id INT UNIQUE NOT NULL REFERENCES affiliates (id) ON DELETE CASCADE)`)
    if err != nil {
        panic(err)
    }
    affiliates := []any{"Jon Doe", "Connor McGregor", "John Jones"}
    _, err = db.Exec(context.TODO(), `INSERT INTO affiliates (name) VALUES ($1), ($2), ($3)`, affiliates...)
    if err != nil {
        panic(err)
    }
    deals := []any{1, 100}
    _, err = db.Exec(context.TODO(), `INSERT INTO deals (affiliate_id, value) VALUES ($1, $2)`, deals...)
    if err != nil {
        panic(err)
    }
}
