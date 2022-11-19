package kormongo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/kamalshkeir/kinput"
	"github.com/kamalshkeir/klog"
)

const (
	Red     = "\033[1;31m%v\033[0m\n"
	Green   = "\033[1;32m%v\033[0m\n"
	Yellow  = "\033[1;33m%v\033[0m\n"
	Blue    = "\033[1;34m%v\033[0m\n"
	Magenta = "\033[5;35m%v\033[0m\n"
)

var dbInUse string

const helpS string = `Commands :  
[databases, use, tables, columns, createsuperuser, createuser, getall, get, drop, delete, clear/cls, q/quit/exit, help/commands]
  'databases':
	  list all connected databases

  'use':
	  use a specific database

  'tables':
	  list all tables in database

  'columns':
	  list all columns of a table

  'getall':
	  get all rows given a table name

  'get':
	  get single row wher field equal_to

  'delete':
	  delete rows where field equal_to

  'drop':
	  drop a table given table name

  'clear/cls':
	  clear console
`

const commandsS string = "Commands :  [databases, use, tables, columns, getall, get, drop, delete, clear/cls, q!/quit/exit]"

// InitShell init the shell and return true if used to stop main
func InitShell() bool {
	args := os.Args
	if len(args) < 2 {
		return false
	}

	switch args[1] {
	case "mongoshell":
		databases := GetMemoryDatabases()
		if len(databases) == 0 {
			klog.Printfs("rdno database connected\n")
			return true
		}
		if len(databases) > 1 {
			var err error
			dbInUse,err =  kinput.String(kinput.Yellow,"Database to use: ")
			if err != nil {
				klog.Printfs("rd%v\n",err)
				return true
			} else if dbInUse == "" {
				klog.Printfs("rdno database given\n")
				return true
			}
		} else if len(databases) == 1 {
			dbInUse=databases[0].Name
		}
		fmt.Printf(Yellow, commandsS)
		for {
			command, err := kinput.String(kinput.Blue, "> ")
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Printf(Blue, "shell shutting down")
				}
				return true
			}

			switch command {
			case "quit", "exit", "q", "q!":
				return true
			case "clear", "cls":
				kinput.Clear()
				fmt.Printf(Yellow, commandsS)
			case "help":
				fmt.Printf(Yellow, helpS)
			case "commands":
				fmt.Printf(Yellow, commandsS)
			case "databases":
				fmt.Printf(Green, GetMemoryDatabases())
			case "use":
				db := kinput.Input(kinput.Blue, "database name: ")
				dbInUse=db
				fmt.Printf(Green, "you are using database "+db)
			case "tables":
				fmt.Printf(Green, GetAllTables(dbInUse))
			case "columns":
				tb := kinput.Input(kinput.Blue, "Table name: ")
				mcols := GetAllColumnsTypes(tb, dbInUse)
				cols := []string{}
				for k := range mcols {
					cols = append(cols, k)
				}
				fmt.Printf(Green, cols)
			case "getall":
				getAll()
			case "get":
				getRow()
			case "drop":
				dropTable()
			case "delete":
				deleteRow()
			default:
				fmt.Printf(Red, "command not handled, use 'help' or 'commands' to list available commands ")
			}
		}
	default:
		return false
	}
}

func getAll() {
	tableName, err := kinput.String(kinput.Blue, "Enter a table name: ")
	if err == nil {
		data, err := Table(tableName).Database(dbInUse).All()
		if err == nil {
			d, _ := json.MarshalIndent(data, "", "    ")
			fmt.Printf(Green, string(d))
		} else {
			fmt.Printf(Red, err.Error())
		}
	} else {
		fmt.Printf(Red, "table name invalid")
	}
}

func getRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name : ")
	whereField := kinput.Input(kinput.Blue, "Where field : ")
	equalTo := kinput.Input(kinput.Blue, "Equal to : ")
	if tableName != "" && whereField != "" && equalTo != "" {
		var data map[string]interface{}
		var err error
		data, err = Table(tableName).Database(dbInUse).Where(whereField, equalTo).One()
		if err == nil {
			d, _ := json.MarshalIndent(data, "", "    ")
			fmt.Printf(Green, string(d))
		} else {
			fmt.Printf(Red, "error: "+err.Error())
		}
	} else {
		fmt.Printf(Red, "One or more field are empty")
	}
}

func dropTable() {
	tableName := kinput.Input(kinput.Blue, "Table to drop : ")
	if tableName != "" {
		_, err := Table(tableName).Database(dbInUse).Drop()
		if err != nil {
			fmt.Printf(Red, "error dropping table :"+err.Error())
		} else {
			fmt.Printf(Green, tableName+" dropped with success")
		}
	} else {
		fmt.Printf(Red, "table is empty")
	}
}

func deleteRow() {
	tableName := kinput.Input(kinput.Blue, "Table Name: ")
	whereField := kinput.Input(kinput.Blue, "Where Field: ")
	equalTo := kinput.Input(kinput.Blue, "Equal to: ")
	if tableName != "" && whereField != "" && equalTo != "" {
		equal, err := strconv.Atoi(equalTo)
		if err != nil {
			_, err := Table(tableName).Database(dbInUse).Where(whereField+" = ?", equalTo).Delete()
			if err == nil {
				fmt.Printf(Green, tableName+"with"+whereField+"="+equalTo+"deleted.")
			} else {
				fmt.Printf(Red, "error deleting row: "+err.Error())
			}
		} else {
			_, err = Table(tableName).Where(whereField+" = ?", equal).Delete()
			if err == nil {
				fmt.Printf(Green, tableName+" with "+whereField+" = "+equalTo+" deleted.")
			} else {
				fmt.Printf(Red, "error deleting row: "+err.Error())
			}
		}
	} else {
		fmt.Printf(Red, "some of args are empty")
	}
}
