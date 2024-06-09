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
	directory string
}

func (f *FileMigrationProvider) GetMigrations() []Migration {
	files, err := os.ReadDir(f.directory)
	if err != nil {
		log.Fatal(err)
	}
	var migrations []Migration
	for _, file := range files {
		if isValidFileName(file.Name()) {
			migration := readMigrationFromFile(f.directory, file.Name())
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
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}
	id := strings.Split(fileName, ".")[0]
	return Migration{Id: id, Statements: lines}
}

