package pgmigrate

import (
	"reflect"
	"testing"
)

func TestMigrationProvider(t *testing.T) {
	provider := &FileMigrationProvider{"testdata"}

	t.Run("read migrations from files", func(t *testing.T) {
		got := provider.GetMigrations()
		want := []Migration{
			{
				Id: "000",
				Statements: []string{
					"create role test_user;",
					"create database test_database;",
				},
			},
			{
				Id: "001",
				Statements: []string{
					"create table cars (brand varchar(255));",
				},
			},
			{
				Id: "002",
				Statements: []string{
					"create table chairs ( brand varchar(255) );",
					"create table tables ( brand varchar(255) );",
					"create table sofas (brand varchar(255));",
				},
			},
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
