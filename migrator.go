package pgmigrate

import (
	"database/sql"
	"fmt"
	"log"
	"slices"
	"time"
)

func RunMigrations(
	session *sql.DB,
	migrations []Migration,
	retryAfterSeconds int,
) (completed []string, err error) {
	if err = initMigrationsTable(session); err != nil {
		err = fmt.Errorf("failed to create migrations table: %v", err)
		return
	}

	records, err := getAllRecords(session)
	if err != nil {
		err = fmt.Errorf("failed to read migrations: %v", err)
		return
	}

	if startedRecords, latest := getStartedRecords(records); len(startedRecords) > 0 {
		secondsSinceLatest := getCurrentTime(session).Sub(*latest.startedAt).Seconds()
		if retryAfterSeconds < 0 || float64(retryAfterSeconds) > secondsSinceLatest {
			var ids []string
			for _, r := range startedRecords {
				ids = append(ids, r.id)
			}
			err = InProgressMigrationsError{Ids: ids, SecondsSinceLatest: secondsSinceLatest}
			return
		}
	}

	for _, m := range migrations {
		if isCompleted(records, m.Id) {
			continue
		}
		markAsStarted(session, m.Id, getCurrentTime(session))
		for i, s := range m.Statements {
			if _, err = session.Exec(s); err != nil {
				err = fmt.Errorf("failed to process statement %d in migration %s: %s", i, m.Id, err)
				return
			}
		}
		markAsCompleted(session, m.Id, getCurrentTime(session))
		completed = append(completed, m.Id)
	}
	return
}

func initMigrationsTable(session *sql.DB) error {
	query := fmt.Sprintf("create table if not exists migrations(id varchar(255) primary key, started_at timestamptz, completed_at timestamptz);")
	if _, err := session.Exec(query); err != nil {
		return err
	}
	return nil
}

func getStartedRecords(allRecords []record) (records []record, latest *record) {
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

type record struct {
	id          string
	startedAt   *time.Time
	completedAt *time.Time
}

func getAllRecords(session *sql.DB) (migrations []record, err error) {
	q := "select id, started_at, completed_at from migrations"
	rows, err := session.Query(q)
	if err != nil {
		err = fmt.Errorf("failed to get in progress rows: %s", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id          string
			startedAt   *time.Time
			completedAt *time.Time
		)
		if err = rows.Scan(&id, &startedAt, &completedAt); err != nil {
			err = fmt.Errorf("failed to scan rows in migration table: %s", err)
			return
		}
		migrations = append(migrations, record{
			id:          id,
			startedAt:   startedAt,
			completedAt: completedAt,
		})
	}
	err = rows.Err()
	return
}

func isCompleted(records []record, id string) bool {
	return slices.ContainsFunc(records, func(r record) bool {
		return r.id == id && r.completedAt != nil
	})
}

func markAsStarted(session *sql.DB, migrationId string, currentTime time.Time) {
	q := "insert into migrations (id, started_at) values ($1, $2) on conflict (id) do update set started_at = $2;"
	if _, err := session.Exec(q, migrationId, currentTime); err != nil {
		log.Fatalf("failed to mark migration %s as processed: %s", migrationId, err)
	}
}

func markAsCompleted(session *sql.DB, migrationId string, currentTime time.Time) {
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

type InProgressMigrationsError struct {
	Ids                []string
	SecondsSinceLatest float64
}

func (e InProgressMigrationsError) Error() string {
	return fmt.Sprintf("migrations with ids %v are in progress with the most recent started %f seconds ago", e.Ids, e.SecondsSinceLatest)
}
