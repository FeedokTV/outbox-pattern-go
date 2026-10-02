package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	connectionString = "postgresql://postgres:passpass1234@localhost:5432/bank-database"
)

func GetDatabaseConnectionReplication(ctx context.Context) (*pgconn.PgConn, error) {

	connectionStringReplication := connectionString + "?replication=database"

	conn, err := pgconn.Connect(ctx, connectionStringReplication)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func GetDatabasePool(ctx context.Context) (*pgxpool.Pool, error) {
	// Let the container name and port stay hardcodeds

	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
