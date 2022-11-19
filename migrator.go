package kormongo

import (
	"context"
	"errors"

	"github.com/kamalshkeir/kmongodriver"
)

func AutoMigrate[T comparable](tableName string, dbName ...string) error {
	if _, ok := mModelTablename[*new(T)]; !ok {
		mModelTablename[*new(T)] = tableName
	}
	var db *DatabaseEntity
	var err error
	dbname := ""
	if len(dbName) == 1 {
		dbname = dbName[0]
		db, err = GetMemoryDatabase(dbname)
		if err != nil || db == nil {
			return errors.New("database not found")
		}
	} else if len(dbName) == 0 {
		dbname = databases[0].Name
		db, err = GetMemoryDatabase(dbname)
		if err != nil || db == nil {
			return errors.New("database not found")
		}
	} else {
		return errors.New("cannot migrate more than one database at the same time")
	}

	tbFoundDB := false
	tables := GetAllTables(dbname)
	for _, t := range tables {
		if t == tableName {
			tbFoundDB = true
		}
	}
	if !tbFoundDB {
		kmongodriver.CreateRow(context.Background(), tableName, new(T))
	}
	return nil
}
