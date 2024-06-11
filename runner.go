package pgmigrate

import (
	"log"
	"strconv"
	_ "github.com/lib/pq"
	"database/sql"
	"fmt"
	"github.com/emillamm/pgmigrate/env"
)

func Run() {
	user := env.GetenvOrFatal("POSTGRES_USER")
	password := env.GetenvOrFatal("POSTGRES_PASSWORD")
	host := env.GetenvWithDefault("POSTGRES_HOST", "localhost")
	port, err := strconv.Atoi(env.GetenvWithDefault("POSTGRES_PORT", "5432"))
	if err != nil {
		log.Fatalf("invalid PORT %d", port)
	}
	migrationDir := env.GetenvWithDefault("POSTGRES_MIGRATION_DIR", "migrations")
	retryAfterSeconds, err := strconv.Atoi(env.GetenvWithDefault("POSTGRES_MIGRATION_RETRY_INTERVAL", "120"))
	if err != nil {
		log.Fatalf("invalid POSTGRES_MIGRATION_RETRY_INTERVAL %d", port)
	}

	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d sslmode=disable", user, password, host, port)
	session, err := sql.Open("postgres", connStr)
	if err == nil {
		err = session.Ping()
	}
	if err != nil {
		log.Fatalf("unable to connect to postgres database: %v", err)
	}

	provider := FileMigrationProvider{Directory: migrationDir}
	migrations := provider.GetMigrations()
	completed, err := RunMigrations(session, migrations, retryAfterSeconds)
	log.Printf("completed %d migrations: %v\n", len(completed), completed)
	if err != nil {
		log.Fatalf("unable to complete some or all migrations: %v", err)
	}
}

