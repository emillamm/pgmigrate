package pgmigrate

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/emillamm/pgmigrate/env"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func Run() {
	user := env.GetenvWithDefault("POSTGRES_USER", "")
	// POSTGRES_PASS and POSTGRES_PASSWORD are both valid keys
	password := env.GetenvWithDefault("POSTGRES_PASSWORD", env.GetenvWithDefault("POSTGRES_PASS", ""))
	host := env.GetenvWithDefault("POSTGRES_HOST", "localhost")
	portStr := env.GetenvWithDefault("POSTGRES_PORT", "5432")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("invalid PORT %s", portStr)
	}
	database := env.GetenvWithDefault("POSTGRES_DATABASE", "postgres")
	migrationDir := env.GetenvWithDefault("POSTGRES_MIGRATION_DIR", "migrations")
	retryAfterSeconds, err := strconv.Atoi(env.GetenvWithDefault("POSTGRES_MIGRATION_RETRY_INTERVAL", "120"))
	if err != nil {
		log.Fatalf("invalid POSTGRES_MIGRATION_RETRY_INTERVAL %d", port)
	}

	session, err := getSession(user, password, host, database, port)
	if err == nil {
		err = session.Ping()
	}
	if err != nil {
		log.Fatalf("unable to connect to postgres database with user=%s host=%s port=%d database=%s: %v", user, host, port, database, err)
	}

	provider := FileMigrationProvider{Directory: migrationDir}
	migrations := provider.GetMigrations()
	completed, err := RunMigrations(session, migrations, retryAfterSeconds)
	log.Printf("completed %d migrations: %v\n", len(completed), completed)
	if err != nil {
		log.Fatalf("unable to complete some or all migrations: %v", err)
	}
}

func getSession(user, password, host, database string, port int) (session *sql.DB, err error) {
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d database=%s sslmode=disable", user, password, host, port, database)
	for i := 0; i < 4; i++ {
		session, err = sql.Open("pgx", connStr)
		if err == nil {
			err = session.Ping()
		}
		if err != nil {
			log.Println(fmt.Sprintf("failed to connect to postgres..."))
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}
	return
}
