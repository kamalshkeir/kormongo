package kormongo

import (
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kmongodriver"
	"github.com/kamalshkeir/kmux/ws"
	"github.com/kamalshkeir/ksbus"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	// Debug when true show extra useful logs for queries executed for migrations and queries statements
	Debug = false
	// FlushCacheEvery execute korm.FlushCache() every 30 min by default, you should not worry about it, but useful that you can change it
	FlushCacheEvery = 30 * time.Minute
	// DefaultDB keep tracking of the first database connected
	DefaultDB         = ""
	useCache          = true
	databases         = []DatabaseEntity{}
	mModelTablename   = map[any]string{}
	cacheGetAllTables = kmap.New[string, []string](false)
	cachesOneM        = kmap.New[dbCache, map[string]any](false)
	cachesAllM        = kmap.New[dbCache, []map[string]any](false)

	onceDone = false
	cachebus *ksbus.Bus
)

const (
	CACHE_TOPIC      = "internal-db-cache"
)

type TableEntity struct {
	Pk         string
	Name       string
	Columns    []string
	Types      map[string]string
	ModelTypes map[string]string
	Tags       map[string][]string
}

type DatabaseEntity struct {
	Name      string
	MongoConn *mongo.Database
	Tables    []TableEntity
}


func New(dbName string,dbDSN ...string) error {
	dbFound := false
	for _, dbb := range databases {
		if dbb.Name == dbName {
			dbFound = true
		}
	}
	if !dbFound {
		mc, err := kmongodriver.NewMongoFromDSN(dbName, dbDSN...)
		if !klog.CheckError(err) {
			databases = append(databases, DatabaseEntity{
				Name:      dbName,
				MongoConn: mc,
				Tables:    []TableEntity{},
			})
		}
	}
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) {
				handleCache(data)
			})
			go RunEvery(FlushCacheEvery, func() {
				go cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		runned := InitShell()
		if runned {
			os.Exit(0)
		}
		onceDone = true
	}
	return nil
}

func NewFromConnection(dbName string,dbConn *mongo.Database) error {
	_,dbFound := GetConnection(dbName)
	if !dbFound {
		databases = append(databases, DatabaseEntity{
			Name:      dbName,
			MongoConn: dbConn,
			Tables:    []TableEntity{},
		})
	}
	if !onceDone {
		if useCache {
			cachebus = ksbus.New()
			cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) {
				handleCache(data)
			})
			go RunEvery(FlushCacheEvery, func() {
				go cachebus.Publish(CACHE_TOPIC, map[string]any{
					"type": "clean",
				})
			})
		}
		runned := InitShell()
		if runned {
			os.Exit(0)
		}
		onceDone = true
	}
	return nil
}


// WithBus take ksbus.NewServer() that can be Run, RunTLS, RunAutoTLS
func WithBus(bus *ksbus.Server) *ksbus.Server {
	cachebus = bus.Bus
	if useCache {
		cachebus.Subscribe(CACHE_TOPIC, func(data map[string]any, ch ksbus.Channel) { handleCache(data) })
		go RunEvery(FlushCacheEvery, func() {
			cachebus.Publish(CACHE_TOPIC, map[string]any{
				"type": "clean",
			})
		})
	}
	return bus
}

// BeforeServersData handle connections and data received from another server
func BeforeServersData(fn func(data any, conn *ws.Conn)) {
	ksbus.BeforeServersData = fn
}

// BeforeDataWS handle connections and data received before upgrading websockets, useful to handle authentication
func BeforeDataWS(fn func(data map[string]any, conn *ws.Conn, originalRequest *http.Request) bool) {
	ksbus.BeforeDataWS = fn
}

// FlushCache send msg to the cache system to Flush all the cache, safe to use in concurrent mode, and safe to use in general, it's done every 30 minutes(korm.FlushCacheEvery) and on update , create, delete , drop
func FlushCache() {
	go cachebus.Publish(CACHE_TOPIC, map[string]any{
		"type": "clean",
	})
}


// DisableCache disable the cache system, if you are having problem with it, you can korm.FlushCache on command too
func DisableCache() {
	useCache = false
}

// GetConnection common way to get a connection,returned as *mongo.Database with ok bool
func GetConnection(dbName ...string) (*mongo.Database, bool) {
	var db *DatabaseEntity
	var err error
	if len(dbName) > 0 {
		db, err = GetMemoryDatabase(dbName[0])
	} else {
		db, err = GetMemoryDatabase(databases[0].Name)
	}
	if klog.CheckError(err) {
		return nil, false
	}
	if db.MongoConn != nil {
		return db.MongoConn, true
	}
	return nil, false
}

// GetAllTables get all tables for the optional dbName given, otherwise, if not args, it will return tables of the first connected database
func GetAllTables(dbName ...string) []string {
	var name string
	if len(dbName) == 0 {
		name = databases[0].Name
	} else {
		name = dbName[0]
	}
	if useCache {
		if v, ok := cacheGetAllTables.Get(name); ok {
			return v
		}
	}
	tables := kmongodriver.GetAllTables(dbName...)
	if useCache && len(tables) > 0 {
		cacheGetAllTables.Set(name, tables)
	}
	return tables
}

// GetAllColumnsTypes get columns and types from the database
func GetAllColumnsTypes(table string, dbName ...string) map[string]string {
	cols := kmongodriver.GetAllColumns(table, dbName...)
	if len(cols) > 0 {
		return cols
	}
	return nil
}

// GetMemoryTable get a table from memory for specified or first connected db
func GetMemoryTable(tbName string, dbName ...string) (TableEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return TableEntity{}, err
	}
	for _, t := range db.Tables {
		if t.Name == tbName {
			return t, nil
		}
	}
	return TableEntity{}, errors.New("nothing found")
}

// GetMemoryTable get all tables from memory for specified or first connected db
func GetMemoryTables(dbName ...string) ([]TableEntity, error) {
	dName := databases[0].Name
	if len(dbName) > 0 {
		dName = dbName[0]
	}
	db, err := GetMemoryDatabase(dName)
	if err != nil {
		return nil, err
	}
	return db.Tables, nil
}

// GetMemoryDatabases get all databases from memory
func GetMemoryDatabases() []DatabaseEntity {
	return databases
}

// GetMemoryDatabase return the first connected database korm.DefaultDatabase if dbName "" or "default" else the matched db
func GetMemoryDatabase(dbName string) (*DatabaseEntity, error) {
	if DefaultDB == "" {
		DefaultDB = databases[0].Name
	}
	switch dbName {
	case "", "default":
		for i := range databases {
			if databases[i].Name == DefaultDB {
				return &databases[i], nil
			}
		}
		return nil, errors.New(dbName + "database not found")
	default:
		for i := range databases {
			if databases[i].Name == dbName {
				return &databases[i], nil
			}
		}
		return nil, errors.New(dbName + "database not found")
	}
}

// ShutdownDatabases shutdown many database, and detect if sql and mongo
func ShutdownDatabases(databasesName ...string) error {
	dbs := []string{}
	for _,db := range databases {
		dbs = append(dbs, db.Name)
	}
	return kmongodriver.ShutdownDatabases(dbs...)
}
