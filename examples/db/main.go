package main

import (
    "context"
    "fmt"

    db "github.com/diegodesousas/go-devkit/pkg/database/sql"
)

type Affiliate struct {
    Id   int
    Name string
}

func main() {
    config := db.Config{
        Host:     "localhost",
        Port:     "5432",
        User:     "postgres",
        Password: "test",
        Database: "test",
        SSLMode:  "disable",
    }
    dbConn, err := db.New(config)
    if err != nil {
        panic(err)
    }

    if err := dbConn.Ping(); err != nil {
        panic(err)
    }

    ctx := context.Background()

    query := "CREATE TABLE IF NOT EXISTS affiliates (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)"
    _, err = dbConn.Exec(ctx, query)
    if err != nil {
        panic(err)
    }

    query = "INSERT INTO affiliates (name) VALUES ('Jon Jones'), ('Connor McGregor')"
    res, err := dbConn.Exec(ctx, query)
    if err != nil {
        panic(err)
    }
    num, _ := res.RowsAffected()
    fmt.Println("Number of rows affected = ", num)

    aff := &Affiliate{}
    query = `SELECT id, name FROM affiliates WHERE name = $1`
    err = dbConn.Get(ctx, aff, query, "Jon Jones")
    if err != nil {
        panic(err)
    }
    fmt.Println("Got affiliate = ", aff)

    affs := &[]Affiliate{}
    query = `SELECT id, name FROM affiliates LIMIT 2`
    err = dbConn.Select(ctx, affs, query)
    if err != nil {
        panic(err)
    }
    fmt.Println("Got affiliates = ", affs)

}
