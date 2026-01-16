package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func baseConnString(cfg Config, dbName string) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		dbName,
		cfg.SSLMode,
	)
}

func EnsureDatabaseExistsStrict(ctx context.Context, cfg Config) error {
	db, err := sql.Open("postgres", baseConnString(cfg, "postgres"))
	if err != nil {
		return err
	}
	defer db.Close()

	var exists bool
	query := `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`
	if err := db.QueryRowContext(ctx, query, cfg.DBName).Scan(&exists); err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("database %q does not exist", cfg.DBName)
	}

	return nil
}


func Connect(cfg Config) (*sql.DB, error) {
	return sql.Open("postgres", baseConnString(cfg, cfg.DBName))
}

func EnsureSchemaExists(ctx context.Context, db *sql.DB, schema string) error {
	var exists bool

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.schemata
			WHERE schema_name = $1
		)
	`
	if err := db.QueryRowContext(ctx, query, schema).Scan(&exists); err != nil {
		return err
	}

	if exists {
		return nil
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA "%s"`, schema))
	return err
}

