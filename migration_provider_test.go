package pgmigrate

import (
	"testing"
	"reflect"
)

func TestMigrationProvider(t *testing.T) {

	provider := &FileMigrationProvider{"testdata"}

	t.Run("read migrations from files", func(t *testing.T) {
		got := provider.GetMigrations()
		want := []Migration{
			Migration{
				Id: "000",
				Statements: []string{
					"create role test_user;",
					"create database test_database;",
				},
			},
			Migration{
				Id: "001",
				Statements: []string{
					"create table cars (brand varchar(255));",
				},
			},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

