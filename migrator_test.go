package pgmigrate

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/emillamm/pgmigrate/env"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestMigrate(t *testing.T) {
	user := env.GetenvWithDefault("POSTGRES_USER", "postgres")
	password := env.GetenvWithDefault("POSTGRES_PASSWORD", "postgres")
	host := env.GetenvWithDefault("POSTGRES_HOST", "localhost")
	portStr := env.GetenvWithDefault("POSTGRES_PORT", "5432")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Errorf("invalid PORT %s", portStr)
		return
	}
	database := env.GetenvWithDefault("POSTGRES_DATABASE", "postgres")

	// Set up parent connection
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d database=%s sslmode=disable", user, password, host, port, database)
	db, err := openConnection(connStr)
	if err != nil {
		t.Errorf("failed to open connection %s", err)
		return
	}
	defer db.Close()

	t.Run("RunMigrations should create a migration table if it doesn't exist", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			// Check that table doesn't exist
			verifyTableExistence(t, session, "migrations", false)

			// Perform some migration
			migrations := []Migration{
				{
					Id: "001",
					Statements: []string{
						"create table test_table(id text)",
					},
				},
			}
			if _, err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("unable to run migrations: %s", err)
			}

			//// Check that table does exist
			verifyTableExistence(t, session, "migrations", true)
		})
	})

	t.Run("RunMigrations should skip a migration if it was already processed", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				{
					Id: "001",
					Statements: []string{
						"create table test_table(id text)",
					},
				},
			}
			if _, err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("unable to run migrations: %s", err)
			}
			if _, err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("expected no error when skipping existing migration but got %s", err)
			}
		})
	})

	t.Run("RunMigrations should return an error if some migrations are in-progress", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				{
					Id: "001",
					Statements: []string{
						"create table test_table1(id text)",
					},
				},
				{
					Id: "002",
					Statements: []string{
						"create table test_table2(id text)",
					},
				},
				{
					Id: "003",
					Statements: []string{
						"create table test_table3(id text)",
					},
				},
				{
					Id: "004",
					Statements: []string{
						"create table test_table4(id text)",
					},
				},
			}

			initMigrationsTable(session)
			markAsCompleted(session, "001", getCurrentTime(session))
			markAsStarted(session, "002", getCurrentTime(session))
			markAsStarted(session, "003", getCurrentTime(session))

			completed, err := RunMigrations(session, migrations, -1)
			startedErr, ok := err.(InProgressMigrationsError)
			if !ok {
				t.Errorf("expected InProgressMigrationsError but got %v", err)
				return
			}
			if startedErr.Ids[0] != "002" || startedErr.Ids[1] != "003" {
				t.Errorf("InProgressMigrationsError did not contain expected migrations 002 and 003: %v", err)
			}
			if len(completed) > 0 {
				t.Errorf("expected no completed migrations")
			}
		})
	})

	t.Run("RunMigrations should skip in-progress migrations that are older than a specified duration", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				{
					Id: "001",
					Statements: []string{
						"create table test_table1(id text)",
					},
				},
				{
					Id: "002",
					Statements: []string{
						"create table test_table2(id text)",
					},
				},
			}
			initMigrationsTable(session)
			startedAt := getCurrentTime(session).Add(-10 * time.Second)
			markAsStarted(session, "001", startedAt)
			completed, err := RunMigrations(session, migrations, 5)
			if err != nil {
				t.Errorf("failed to ignore in-progress migration: %v", err)
			}
			if len(completed) != 2 {
				t.Errorf("expected 2 completed migrations but got %d", len(completed))
			}
		})
	})

	t.Run("RunMigrations should complete all migrations", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			// Helper to verify inserted records
			verifyRecords := func(expectedNumberOfRecords int) {
				// Verify number or records
				allRecords, err := getAllRecords(session)
				if err != nil {
					t.Errorf("unable to get migration records")
				}
				if len(allRecords) != expectedNumberOfRecords {
					t.Errorf("unexpected number of records: got %d, wanted %d", len(allRecords), expectedNumberOfRecords)
				}

				// Verify each record
				for i, r := range allRecords {
					if r.id != fmt.Sprintf("00%d", i+1) || r.startedAt == nil || r.completedAt == nil {
						t.Errorf("got %v, wanted id=00%d, startedAt!=nil, completedAt!=nil", r, i+1)
					}
				}
			}

			migrations := []Migration{
				{
					Id: "001",
					Statements: []string{
						"create table test_table1(id text)",
					},
				},
				{
					Id: "002",
					Statements: []string{
						"create table test_table2(id text)",
						"create table test_table3(id text)",
					},
				},
			}
			completed, err := RunMigrations(session, migrations, -1)
			if err != nil {
				t.Errorf("failed to run migrations: %v", err)
			}
			if len(completed) != 2 {
				t.Errorf("expected 2 completed migrations but got %d", len(completed))
			}

			// Verify all three tables exist
			verifyTableExistence(t, session, "test_table1", true)
			verifyTableExistence(t, session, "test_table2", true)
			verifyTableExistence(t, session, "test_table3", true)

			// Verify records
			verifyRecords(2)

			// Add another migration
			migrations = append(migrations, Migration{
				Id: "003",
				Statements: []string{
					"create table test_table4(id text)",
				},
			})

			completed, err = RunMigrations(session, migrations, -1)
			if err != nil {
				t.Errorf("failed to run migrations again: %v", err)
			}
			if len(completed) != 1 {
				t.Errorf("expected 1 completed migration but got %d", len(completed))
			}

			// Verify again
			verifyTableExistence(t, session, "test_table4", true)
			verifyRecords(3)
		})
	})
}

// -- Helper methods

func verifyTableExistence(
	t testing.TB,
	session *sql.DB,
	tableName string,
	shouldExist bool,
) {
	q := fmt.Sprintf("select from information_schema.tables where table_name = '%s' and table_schema = 'public';", tableName)
	row := session.QueryRow(q)
	doesExist := true
	if err := row.Scan(); err != nil {
		if err != sql.ErrNoRows {
			t.Errorf("invalid error: %s", err)
		}
		doesExist = false
	}
	if doesExist != shouldExist {
		t.Errorf("table %s existence=%t not valid", tableName, shouldExist)
	}
}

func ephemeralSession(
	t testing.TB,
	parentSession *sql.DB,
	host string,
	port int,
	block func(session *sql.DB),
) {
	t.Helper()

	user := randomUser()
	password := "test"

	createRoleQ := fmt.Sprintf("create role %s with login password '%s';", user, password)
	if _, err := parentSession.Exec(createRoleQ); err != nil {
		t.Errorf("failed to create role %s: %s", user, err)
		return
	}

	createDbQ := fmt.Sprintf("create database %s owner %s;", user, user)
	if _, err := parentSession.Exec(createDbQ); err != nil {
		t.Errorf("failed to create database %s: %s", user, err)
		return
	}

	defer func() {
		dropDbQ := fmt.Sprintf("drop database %s;", user)
		if _, err := parentSession.Exec(dropDbQ); err != nil {
			t.Errorf("failed to drop database %s: %s", user, err)
			return
		}
		dropRoleQ := fmt.Sprintf("drop role %s;", user)
		if _, err := parentSession.Exec(dropRoleQ); err != nil {
			t.Errorf("failed to drop role %s: %s", user, err)
			return
		}
	}()

	// Set up connection
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d database=%s sslmode=disable", user, password, host, port, user)
	session, err := openConnection(connStr)
	if err != nil {
		t.Errorf("failed to open connection %s", err)
		return
	}
	defer session.Close()
	block(session)
}

func openConnection(connStr string) (db *sql.DB, err error) {
	db, err = sql.Open("pgx", connStr)
	if db != nil {
		err = db.Ping()
	}
	return
}

// Generates user/DB name in the form of "test_[a-z]7" e.g. test_hqbrluz
func randomUser() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	length := 7
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return fmt.Sprintf("test_%s", string(b))
}
