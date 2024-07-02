package pgmigrate

import (
	"log"
	"os"
	"bufio"
	"fmt"
	"strings"
	"regexp"
)

type Migration struct {
	Id string
	Statements []string
}

type MigrationProvider interface {
	GetMigrations() []Migration
}

type FileMigrationProvider struct {
	Directory string
}

func (f *FileMigrationProvider) GetMigrations() []Migration {
	files, err := os.ReadDir(f.Directory)
	if err != nil {
		log.Fatalf("unable to read files from '%s' folder: %v", f.Directory, err)
	}
	var migrations []Migration
	for _, file := range files {
		if isValidFileName(file.Name()) {
			migration := readMigrationFromFile(f.Directory, file.Name())
			migrations= append(migrations, migration)
		}
	}
	return migrations
}

const validFileName = ".+\\.sql"
func isValidFileName(fileName string) bool {
	match, err := regexp.MatchString(validFileName, fileName)
	if err != nil {
		log.Fatal("invalid regex")
	}
	return match
}

func readMigrationFromFile(filePath string, fileName string) Migration {
	fullPath := fmt.Sprintf("%s/%s", filePath, fileName)
	file, err := os.Open(fullPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// scan through lines and concatenate lines that don't end with ';' as statements
	scanner := bufio.NewScanner(file)
	var statements []string
	var statement strings.Builder
	var whitespace string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fmt.Fprintf(&statement, "%s%s", whitespace, line)
		whitespace = " "
		if line[len(line)-1] == ';' {
			statements = append(statements, statement.String())
			statement.Reset()
			whitespace = ""
		}
	}
	id := strings.Split(fileName, ".")[0]
	return Migration{Id: id, Statements: statements}
}

