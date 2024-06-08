package main

import (
	"testing"
	"os"
	"fmt"
	"math/rand"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
	"strconv"
	"time"
)

func TestMigrate(t *testing.T) {

	godotenv.Load("testdata/testconf.env")

	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	port, err := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	if err != nil {
		t.Errorf("invalid port %d", port)
		return
	}

	// Set up connection
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d sslmode=disable", user, password, host, port)
	db, err := openConnection(connStr)
	if err != nil {
		t.Errorf("failed to open connection %s", err)
		return
	}
	defer db.Close()

	//t.Run("Run should update password when AllowUpdatePassword is set and authenticating for the first time", func(t *testing.T) {
	//	//m := NewMigrator()
	//	//m.Run()
	//})

	t.Run("RunMigrations should create a migration table if it doesn't exist", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			// Helper method to check table existence
			checkTableExistence := func(shouldExist bool) {
				q := fmt.Sprintf("select from information_schema.tables where table_name = 'migrations' and table_schema = 'public';")
				row := session.QueryRow(q)
				doesExist := true
				if err := row.Scan(); err != nil {
					if err != sql.ErrNoRows {
						t.Errorf("invalid error: %s", err)
					}
					doesExist = false
				}
				if doesExist != shouldExist {
					t.Errorf("migration table existence=%t not valid", shouldExist)
				}
			}

			// Check that table doesn't exist
			checkTableExistence(false)

			// Perform some migration
			migrations := []Migration{
				Migration{
					Id: "001",
					Statements: []string{
						"create table test_table(id text)",
					},
				},
			}
			if err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("unable to run migrations: %s", err)
			}

			//// Check that table does exist
			checkTableExistence(true)
		})
	})

	t.Run("RunMigrations should skip a migration if it was already processed", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				Migration{
					Id: "001",
					Statements: []string{
						"create table test_table(id text)",
					},
				},
			}
			if err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("unable to run migrations: %s", err)
			}
			//if err := RunMigrations(session, migrations); err != nil {
			//	if _, ok := err.{MigrationAlreadyProcessedError}; !ok {
			//		t.Errorf(": %s", err)
			//	}
			//}
			//if _, ok := RunMigrations(session, migrations).{MigrationAlreadyProcessedError}; !ok {
			//	t.Errorf(": %s", err)
			//}

			//if err := RunMigrations(session, migrations) && _, ok := err.{MigrationAlreadyProcessedError}; !ok {
			//	t.Errorf("invalid error: %s", err)
			//}

			//if _, ok := RunMigrations(session, migrations).(MigrationAlreadyProcessedError); !ok {
			//	t.Errorf("expected MigrationAlreadyProcessedError but got %v", err)
			//}

			// If migration is not skipped, it will return an error because table already exists
			if err := RunMigrations(session, migrations, -1); err != nil {
				t.Errorf("expected no error when skipping existing migration but got %s", err)
			}

			//if err := RunMigrations(session, migrations); err != nil {
			//	if _, ok := err.(MigrationAlreadyProcessedError); !ok {
			//		t.Errorf("invalid error: %s", err)
			//	}
			//} else {
			//	t.Error("no error returned, expected MigrationAlreadyProcessedError")
			//}
		})
	})

	t.Run("RunMigrations should return an error if some migrations are in-progress", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				Migration{
					Id: "001",
					Statements: []string{
						"create table test_table1(id text)",
					},
				},
				Migration{
					Id: "002",
					Statements: []string{
						"create table test_table2(id text)",
					},
				},
				Migration{
					Id: "003",
					Statements: []string{
						"create table test_table3(id text)",
					},
				},
				Migration{
					Id: "004",
					Statements: []string{
						"create table test_table4(id text)",
					},
				},
			}

			initMigrationsTable(session)
			markAsCompleted(session, "001", getCurrentTime(session))
			markAsInProgress(session, "002", getCurrentTime(session))
			markAsInProgress(session, "003", getCurrentTime(session))

			//time.Sleep(5 * time.Second)
			err := RunMigrations(session, migrations, -1)
			inProgressErr, ok := err.(InProgressMigrationsError)
			if !ok {
				t.Errorf("expected InProgressMigrationsError but got %v", err)
				return
			}
			if inProgressErr.Ids[0] != "002" || inProgressErr.Ids[1] != "003" {
				t.Errorf("InProgressMigrationsError did not contain expected migrations 002 and 003: %v", err)
			}

			//v, e := getAllMigrationRecords(session)
			//t.Errorf("e %v, v %v", e,v)
		})
	})

	t.Run("RunMigrations should skip in-progress migrations that are older than a specified duration", func(t *testing.T) {
		ephemeralSession(t, db, host, port, func(session *sql.DB) {
			migrations := []Migration{
				Migration{
					Id: "001",
					Statements: []string{
						"create table test_table1(id text)",
					},
				},
				Migration{
					Id: "002",
					Statements: []string{
						"create table test_table2(id text)",
					},
				},
			}
			initMigrationsTable(session)
			startedAt := getCurrentTime(session).Add(-10 * time.Second)
			markAsInProgress(session, "001", startedAt)
			if err := RunMigrations(session, migrations, 5); err != nil {
				t.Errorf("failed to ignore in-progress migration: %v", err)
			}
		})
	})

	//t.Run("RunMigrations should skip a migration if it was already processed", func(t *testing.T) {
	//	ephemeralSession(t, db, host, port, func(session *sql.DB) {
	//	})
	//})

	//t.Run("RunMigrations should create prevent the same migration from being run twice", func(t *testing.T) {
	//	ephemeralSession(t, parentSession, user, pass, host, func(session *gocql.Session, keyspace string) {
	//		// Perform some migration
	//		migrations := []Migration{
	//			Migration{
	//				Id: "001",
	//				Statements: []string{
	//					"invalidcql",
	//				},
	//			},
	//		}
	//		_ := RunMigrations(session, migrations)

	//	})
	//})
	//t.Run("RunMigrations should execute statements in alphabetical order of id", func(t *testing.T) {
}

func ephemeralSession(
	t testing.TB,
	parentSession *sql.DB,
	host string,
	port int,
	block func(session *sql.DB),
) {
	t.Helper()
	//var err error
	//var session *sql.DB

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

	// Set up connection
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%d sslmode=disable", user, password, host, port)
	session, err := openConnection(connStr)
	if err != nil {
		t.Errorf("failed to open connection %s", err)
		return
	}

	defer func() {
		session.Close()
		dropDbQ := fmt.Sprintf("drop database %s;", user)
		if _, err = parentSession.Exec(dropDbQ); err != nil {
			t.Errorf("failed to drop database %s: %s", user, err)
			return
		}
		dropRoleQ := fmt.Sprintf("drop role %s;", user)
		if _, err = parentSession.Exec(dropRoleQ); err != nil {
			t.Errorf("failed to drop role %s: %s", user, err)
			return
		}
	}()

	block(session)
}

func openConnection(connStr string) (db *sql.DB, err error) {
	db, err = sql.Open("postgres", connStr)
	if db != nil {
		err = db.Ping()
	}
	return
}

// Generates keyspace name in the form of "test_[a-z]7" e.g. test_hqbrluz
func randomUser() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	length := 7
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return fmt.Sprintf("test_%s", string(b))
}

