package kormongo

import (
	"fmt"
	"time"

	"github.com/kamalshkeir/klog"
)

type dbCache struct {
	limit      int
	page       int
	database   string
	table      string
	selected   string
	orderBys   string
	whereQuery string
	query      string
	offset     string
	statement  string
	args       string
}

func getTableName[T comparable]() string {
	if v, ok := mModelTablename[*new(T)]; ok {
		return v
	} else {
		return ""
	}
}

func RunEvery(t time.Duration, function any) {
	//Usage : go RunEvery(2 * time.Second,func(){})
	fn, ok := function.(func())
	if !ok {
		fmt.Println("ERROR : fn is not a function")
		return
	}

	fn()
	c := time.NewTicker(t)

	for range c.C {
		fn()
	}
}

func handleCache(data map[string]any) {
	switch data["type"] {
	case "create","delete","update":
		if v,ok := data["table"];ok {
			dbName := data["database"].(string)
			go func() {
				cachesAllM.Range(func(key dbCache, value []map[string]any) {
					if key.table == v && key.database == dbName {
						cachesAllM.Delete(key)
					}
				})
				cachesAllS.Range(func(key dbCache, value any) {
					if key.table == v && key.database == dbName {
						cachesAllS.Delete(key)
					}
				})
				cachesOneM.Range(func(key dbCache, value map[string]any) {
					if key.table == v && key.database == dbName {
						cachesOneM.Delete(key)
					}
				})
				cachesOneS.Range(func(key dbCache, value any) {
					if key.table == v && key.database == dbName {
						cachesOneS.Delete(key)
					}
				})	
			}()			
		} else {
			go func() {
				cachesAllM.Flush()
				cachesAllS.Flush()
				cachesOneM.Flush()
				cachesOneS.Flush()
			}()
		}
	case "drop":
		go func() {
			cacheGetAllTables.Flush()
			cachesAllM.Flush()
			cachesAllS.Flush()
			cachesOneM.Flush()
			cachesOneS.Flush()
		}()
	case "clean":
		go func() {
			cacheGetAllTables.Flush()
			cachesAllM.Flush()
			cachesAllS.Flush()
			cachesOneM.Flush()
			cachesOneS.Flush()
		}()
	default:
		klog.Printf("CACHE DB: default case triggered %v \n", data)
	}
}
