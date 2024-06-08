package main

import (
	"fmt"
	"database/sql"
	"log"
	"time"
	"slices"
)

//type Migrator struct {
//	provider MigrationProvider
//	//cassandra Cassandra
//	AllowUpdatePassword bool
//}
//
//func (m *Migrator) Run() error {
//	//if m.AllowUpdatePassword {
//	//}
//}

func RunMigrations(
	session *sql.DB,
	migrations []Migration,
	ignoreStaleMigrationsAfterSeconds float64,
) error {

	if err := initMigrationsTable(session); err != nil {
		return fmt.Errorf("failed to create migrations table: %v", err)
	}

	records, err := getAllMigrationRecords(session)
	if err != nil {
		return fmt.Errorf("failed to read migrations: %v", err)
	}

	if inProgressRecords, latest := getInProgressRecords(records); len(inProgressRecords) > 0 {
		secondsSinceLatest := getCurrentTime(session).Sub(*latest.startedAt).Seconds()
		if ignoreStaleMigrationsAfterSeconds > secondsSinceLatest || ignoreStaleMigrationsAfterSeconds < 0 {
			var ids []string
			for _, r := range inProgressRecords {
				ids = append(ids, r.id)
			}
			return InProgressMigrationsError{Ids: ids, SecondsSinceLatest: secondsSinceLatest}
		}
	}

	for _, m := range migrations {
		//isCompleted := slices.ContainsFunc(records, func(r migrationRecord) bool {
		//	r.Id == m.Id && r.completedAt != nil
		//})
		if isCompleted(records, m.Id) {
			break
		}
		markAsInProgress(session, m.Id, getCurrentTime(session))
		for i, s := range m.Statements {
			if _, err := session.Exec(s); err != nil {
				return fmt.Errorf("failed to process statement %d in migration %s: %s", i, m.Id, err)
			}
		}
		markAsCompleted(session, m.Id, getCurrentTime(session))
	}
	return nil
}

func initMigrationsTable(session *sql.DB) error {
	query := fmt.Sprintf("create table if not exists migrations(id varchar(255) primary key, started_at timestamp, completed_at timestamp);")
	if _, err := session.Exec(query); err != nil {
		return err
	}
	return nil
}

func getInProgressRecords(allRecords []migrationRecord) (records []migrationRecord, latest *migrationRecord) {
	for _, r := range allRecords {
		if r.startedAt != nil && r.completedAt == nil {
			records = append(records, r)
			if latest == nil {
				latest = &r
			} else {
				if r.startedAt.After(*latest.startedAt) {
					latest = &r
				}
			}
		}
	}
	return
}

//func getInProgressMigrations(session *sql.DB, until time.Time) []string {
//	q := "select id from migrations where completed_at is null and started_at < $1"
//	rows, err := session.Query(q, until)
//	if err != nil {
//		log.Fatalf("failed to get in progress rows: %s", err)
//	}
//
//}

type migrationRecord struct {
	id string
	startedAt *time.Time
	completedAt *time.Time
}

func getAllMigrationRecords(session *sql.DB) (migrations []migrationRecord, err error) {
	q := "select id, started_at, completed_at from migrations"
	rows, err := session.Query(q)
	if err != nil {
		err = fmt.Errorf("failed to get in progress rows: %s", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id string
			startedAt *time.Time
			completedAt *time.Time
		)
		if err = rows.Scan(&id, &startedAt, &completedAt); err != nil {
			err = fmt.Errorf("failed to scan rows in migration table: %s", err)
			return
		}
		migrations = append(migrations, migrationRecord{
			id: id,
			startedAt: startedAt,
			completedAt: completedAt,
		})
	}
	err = rows.Err()
	return
}

func isCompleted(records []migrationRecord, id string) bool {
	return slices.ContainsFunc(records, func(r migrationRecord) bool {
		return r.id == id && r.completedAt != nil
	})
}

//func scanMigrationRows(rows *sql.Rows) (rows []migrationRow, err error) {
//	defer rows.Close()
//	for rows.Next() {
//		var (
//			id string
//			startedAt time.Time
//			completedAt time.Time
//		)
//		if err := rows.Scan(); err != nil {
//			log.Fatalf("failed to scan rows in migration table: %s", err)
//		}
//
//	}
//}

func isAlreadyProcessed(session *sql.DB, migrationId string) bool {
	q := fmt.Sprintf("select from migrations where id = '%s'", migrationId)
	row := session.QueryRow(q)
	if err := row.Scan(); err != nil {
		if err != sql.ErrNoRows {
			log.Fatal(err)
		}
		return false
	}
	return true
}

func markAsInProgress(session *sql.DB, migrationId string, currentTime time.Time) {
	q := "insert into migrations (id, started_at) values ($1, $2) on conflict (id) do update set started_at = $2;"
	if _, err := session.Exec(q, migrationId, currentTime); err != nil {
		log.Fatalf("failed to mark migration %s as processed: %s", migrationId, err)
	}
}

func markAsCompleted(session *sql.DB, migrationId string, currentTime time.Time) {
	//q := "update migrations set completed_at = $2 where id = $1;"
	q := "insert into migrations (id, completed_at) values ($1, $2) on conflict (id) do update set completed_at = $2;"
	if _, err := session.Exec(q, migrationId, currentTime); err != nil {
		log.Fatalf("failed to mark migration %s as completed: %s", migrationId, err)
	}
}

func getCurrentTime(session *sql.DB) time.Time {
	var ts time.Time
	row := session.QueryRow("select current_timestamp;")
	if err := row.Scan(&ts); err != nil {
		log.Fatal("failed to read timestamp from database")
	}
	return ts
}

type InProgressMigrationsError struct{
	Ids []string
	SecondsSinceLatest float64
}
func (e InProgressMigrationsError) Error() string {
	return fmt.Sprintf("migrations with ids %v are in progress with the most recent started %f seconds ago", e.Ids, e.SecondsSinceLatest)
}

