package pgmigrate

import (
	"reflect"
	"testing"
)

func TestStripComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no comment",
			input: "SELECT * FROM users;",
			want:  "SELECT * FROM users;",
		},
		{
			name:  "full line comment",
			input: "-- this is a comment",
			want:  "",
		},
		{
			name:  "inline comment",
			input: "SELECT * FROM users; -- get all users",
			want:  "SELECT * FROM users;",
		},
		{
			name:  "double dash in single quoted string",
			input: "INSERT INTO products (name) VALUES ('Item -- Special Edition');",
			want:  "INSERT INTO products (name) VALUES ('Item -- Special Edition');",
		},
		{
			name:  "double dash in double quoted identifier",
			input: `SELECT "price--discount" FROM products;`,
			want:  `SELECT "price--discount" FROM products;`,
		},
		{
			name:  "double dash in string with comment after",
			input: "INSERT INTO products (name) VALUES ('Item -- Special'); -- add product",
			want:  "INSERT INTO products (name) VALUES ('Item -- Special');",
		},
		{
			name:  "escaped quote in string",
			input: `INSERT INTO items (name) VALUES ('O\'Reilly -- Books');`,
			want:  `INSERT INTO items (name) VALUES ('O\'Reilly -- Books');`,
		},
		{
			name:  "mixed quotes",
			input: `INSERT INTO items (name, "desc--ion") VALUES ('Test -- Item', 'value'); -- comment`,
			want:  `INSERT INTO items (name, "desc--ion") VALUES ('Test -- Item', 'value');`,
		},
		{
			name:  "only whitespace after stripping comment",
			input: "   -- just a comment",
			want:  "",
		},
		{
			name:  "comment with leading whitespace",
			input: "  SELECT * FROM users;  -- trailing comment",
			want:  "SELECT * FROM users;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripComments(tt.input)
			if got != tt.want {
				t.Errorf("stripComments() = %q, want %q", got, tt.want)
			}
		})
	}
}

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
