package cmd

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sqlMigrator/internal/database"

	"github.com/joho/godotenv"
)

type SQLSection struct {
	Table string
	SQL   string
}

func SqlRunner() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	sqlFile := flag.String("file", "", "SQL file name (relative to sql/migrate folder)")
	runCreate := flag.Bool("create", false, "Run CREATE section")
	runDrop := flag.Bool("drop", false, "Run DROP section")
	generate := flag.Bool("gen", false, "Generate SQL template file")
	allFileFlag := flag.Bool("all", false, "Migrate all files in sql/migrate folder directory")
	flag.Parse()

	if *generate {
		if *sqlFile == "" {
			log.Fatal("-file <file_name> is required with -gen to generate .sql file")
		}

		if err := generateSQLFile(*sqlFile); err != nil {
			fmt.Println("SQL file generation failed")
			log.Fatal(err)
		}

		fmt.Println("SQL file generated successfully")
		return
	}

	if allFileFlag != nil && *allFileFlag {
		if *sqlFile == "" {
			log.Fatal("Please provide a SQL file using -file")
		}
	}

	if *runCreate == *runDrop {
		log.Fatal("Specify exactly one: -create OR -drop")
	}

	cfg := database.Config{
		Host:     getEnv("DB_HOST", "localhost"),
		Port:     getEnvInt("DB_PORT", 5432),
		User:     getEnv("DB_USER", "postgres"),
		Password: getEnv("DB_PASSWORD", "password"),
		DBName:   getEnv("DB_NAME", "postgres"),
		Schema:   getEnv("DB_SCHEMA", "public"),
		SSLMode:  getEnv("DB_SSLMODE", "disable"),
	}

	ctx := context.Background()

	// 1. DB must exist
	if err := database.EnsureDatabaseExistsStrict(ctx, cfg); err != nil {
		log.Fatalf("Database not found: %v", err)
	}

	// 2. Connect to DB
	db, err := database.Connect(cfg)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// 3. Ensure schema exists
	if err := database.EnsureSchemaExists(ctx, db, cfg.Schema); err != nil {
		log.Fatalf("Schema does not exist: %v", err)
	}

	if allFileFlag != nil && *allFileFlag {

		files, err := getFileNames("sql/migrations")
		if err != nil {
			log.Fatalf("Failed to get sql files: %v", err)
		}
		for _, f := range files {
			sqlPath := filepath.Join("sql", "migrations", f)
			sqlBytes, err := os.ReadFile(sqlPath)
			if err != nil {
				log.Fatalf("cannot read SQL file: %v", err)
			}

			sqlToRun, err := extractSQLSection(string(sqlBytes), *runCreate)
			if err != nil {
				log.Fatalf("Failed to get sql query info: %v", err)
			}
			if _, err := db.ExecContext(ctx, applySchemaExact(sqlToRun.SQL, cfg.Schema, sqlToRun.Table, *runCreate)); err != nil {
				log.Fatalf("SQL execution failed: %v", err)
			}
			fmt.Println("SQL executed successfully:", f)

		}
	} else {
		sqlPath := filepath.Join("sql", "migrations", *sqlFile)
		sqlBytes, err := os.ReadFile(sqlPath)
		if err != nil {
			log.Fatalf("cannot read SQL file: %v", err)
		}

		sqlToRun, err := extractSQLSection(string(sqlBytes), *runCreate)
		if err != nil {
			log.Fatalf("Failed to get sql query info: %v", err)
		}

		if _, err := db.ExecContext(ctx, applySchemaExact(sqlToRun.SQL, cfg.Schema, sqlToRun.Table, *runCreate)); err != nil {
			log.Fatalf("SQL execution failed: %v", err)
		}
		fmt.Println("SQL executed successfully:", sqlPath)

	}

}

func extractSQLSection(sql string, isCreate bool) (*SQLSection, error) {
	lines := strings.Split(sql, "\n")

	var (
		start int
		end   = len(lines)
		table string
	)

	for i, line := range lines {
		l := strings.TrimSpace(strings.ToLower(line))

		if strings.HasPrefix(l, "--create:") && isCreate {
			table = strings.TrimSpace(line[len("--create:"):])
			start = i + 1
		}

		if strings.HasPrefix(l, "--drop:") {
			if isCreate {
				end = i
			} else {
				table = strings.TrimSpace(line[len("--drop:"):])
				start = i + 1
			}
			break
		}
	}

	if table == "" {
		return nil, fmt.Errorf("table name not found in SQL header")
	}

	sqlBody := strings.Join(lines[start:end], "\n")
	if strings.TrimSpace(sqlBody) == "" {
		return nil, fmt.Errorf("empty SQL body for table %s", table)
	}

	return &SQLSection{
		Table: table,
		SQL:   sqlBody,
	}, nil
}

func applySchemaExact(sql, schema, table string, operation bool) string {
	qualified := schema + "." + table

	sql = strings.ReplaceAll(sql,
		table,
		qualified,
	)

	if !operation {
		sql = strings.ReplaceAll(sql, ";", "")
		sql = sql + " CASCADE;"
	}
	return sql
}

func generateSQLFile(input string) error {
	// Ensure .sql extension
	filename := input
	if !strings.HasSuffix(filename, ".sql") {
		filename += ".sql"
	}

	// Extract base name for table
	base := strings.TrimSuffix(filename, ".sql")

	// Unix timestamp prefix
	ts := time.Now().Unix()
	finalName := fmt.Sprintf("%d-%s.sql", ts, base)

	// Target directory
	dir := filepath.Join("sql", "migrations")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fullPath := filepath.Join(dir, finalName)

	// Prevent overwrite
	if _, err := os.Stat(fullPath); err == nil {
		return fmt.Errorf("file already exists: %s", fullPath)
	}

	content := fmt.Sprintf(`--create:%s
		--replace table_name with your table
		--write your create query

		--drop:%s
		--replace table_name with your table
		--write your drop query
		`, base, base)

	return os.WriteFile(fullPath, []byte(content), 0644)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func getFileNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}

// func
