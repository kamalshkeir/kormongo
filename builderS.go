package kormongo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/kmongodriver"
)

var cachesOneS = kmap.New[dbCache, any](false)
var cachesAllS = kmap.New[dbCache, any](false)

type Builder[T comparable] struct {
	debug      bool
	limit      int
	page       int
	tableName  string
	selected   string
	orderBys   string
	whereQuery string
	query      string
	offset     string
	statement  string
	database   string
	args       []any
	order      []string
	ctx        context.Context
}

func Model[T comparable](tableName ...string) *Builder[T] {
	tName := getTableName[T]()
	if tName == "" {
		if len(tableName) > 0 {
			mModelTablename[*new(T)] = tableName[0]
			tName = tableName[0]
		} else {
			klog.Printf("rdunable to find tableName from model, restart the app if you just migrated\n")
			return nil
		}
	}
	return &Builder[T]{
		tableName: tName,
	}
}

func BuilderS[T comparable](tableName ...string) *Builder[T] {
	tName := getTableName[T]()
	if tName == "" {
		if len(tableName) > 0 {
			mModelTablename[*new(T)] = tableName[0]
			tName = tableName[0]
		} else {
			klog.Printf("rdunable to find tableName from model, restart the app if you just migrated\n")
			return nil
		}
	}
	return &Builder[T]{
		tableName: tName,
	}
}

func (b *Builder[T]) Database(dbName string) *Builder[T] {
	b.database = dbName
	if b.database == "" {
		b.database = databases[0].Name
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		b.database = databases[0].Name
	} else {
		b.database = db.Name
	}
	return b
}

func (b *Builder[T]) Insert(model *T) (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			return 0, errors.New("unable to find tableName from model, restart the app if you just migrated")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "create",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	if b.ctx == nil {
		b.ctx = context.Background()
	}
	err = kmongodriver.CreateRow(b.ctx, b.tableName, model, db.Name)
	if klog.CheckError(err) {
		return 0, err
	}
	return 1, nil
}

// Set usage: Set("email, is_admin","example@mail.com",true)
func (b *Builder[T]) Set(fieldsCommaSeparated string, args ...any) (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			klog.Printf("rdunable to find tableName from model\n")
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "update",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}

	if b.ctx == nil {
		b.ctx = context.Background()
	}
	wf := map[string]any{}
	if b.whereQuery != "" {
		if strings.Contains(b.whereQuery, ",") {
			sp := strings.Split(b.whereQuery, ",")
			if len(b.args) == len(sp) {
				for i, s := range sp {
					wf[strings.TrimSpace(s)] = b.args[i]
				}
			}
		} else {
			if len(b.args) == 1 {
				wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
			}
		}
	}
	newRow := map[string]any{}
	spp := strings.Split(fieldsCommaSeparated, ",")
	for _, s := range spp {
		seq := strings.Split(s, "=")
		newRow[seq[0]] = seq[1]
	}
	err = kmongodriver.UpdateRow(b.ctx, b.tableName, wf, newRow,db.Name)
	if klog.CheckError(err) {
		return 0, err
	}
	return 1, nil
}

func (b *Builder[T]) Delete() (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			klog.Printf("unable to find tableName from model\n")
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "delete",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	wf := map[string]any{}
	if b.whereQuery != "" {
		if strings.Contains(b.whereQuery, ",") {
			sp := strings.Split(b.whereQuery, ",")
			if len(b.args) == len(sp) {
				for i, s := range sp {
					wf[strings.TrimSpace(s)] = b.args[i]
				}
			}
		} else {
			if len(b.args) == 1 {
				wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
			}
		}
	}
	if b.ctx == nil {
		b.ctx = context.Background()
	}
	err = kmongodriver.DeleteRow(b.ctx, b.tableName, wf, db.Name)
	if klog.CheckError(err) {
		return 0, err
	}
	return 1, nil
}

func (b *Builder[T]) Drop() (int, error) {
	if b.tableName == "" {
		tName := getTableName[T]()
		if tName == "" {
			return 0, errors.New("unable to find tableName from model")
		}
		b.tableName = tName
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		go cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "drop",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if klog.CheckError(err) {
		return 0, err
	}
	if b.ctx == nil {
		b.ctx = context.Background()
	}
	kmongodriver.DropTable(b.ctx, b.tableName, db.Name)
	return 1, nil
}

// Select usage: Select("email","password")
func (b *Builder[T]) Select(columns ...string) *Builder[T] {
	s := []string{}
	s = append(s, columns...)
	b.selected = strings.Join(s, ",")
	b.order = append(b.order, "select")
	return b
}

func (b *Builder[T]) Where(fieldsCommaSeparated string, args ...any) *Builder[T] {
	b.whereQuery = fieldsCommaSeparated
	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

func (b *Builder[T]) Limit(limit int) *Builder[T] {
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

func (b *Builder[T]) Context(ctx context.Context) *Builder[T] {
	b.ctx = ctx
	return b
}

func (b *Builder[T]) Page(pageNumber int) *Builder[T] {
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

func (b *Builder[T]) OrderBy(fields ...string) *Builder[T] {
	if b.database == "" {
		b.database = databases[0].Name
	}
	if _, ok := kmongodriver.MMongoDBS.Get(b.database); ok {
		b.orderBys = strings.Join(fields, ",")
		b.order = append(b.order, "order_by")
		return b
	}
	b.orderBys = "ORDER BY "
	orders := []string{}
	for _, f := range fields {
		if strings.HasPrefix(f, "+") {
			orders = append(orders, f[1:]+" ASC")
		} else if strings.HasPrefix(f, "-") {
			orders = append(orders, f[1:]+" DESC")
		} else {
			orders = append(orders, f+" ASC")
		}
	}
	b.orderBys += strings.Join(orders, ",")
	b.order = append(b.order, "order_by")
	return b
}

func (b *Builder[T]) Debug() *Builder[T] {
	b.debug = true
	return b
}

func (b *Builder[T]) All() ([]T, error) {
	if b.database == "" {
		b.database = databases[0].Name
	}
	if b.tableName == "" {
		return nil, errors.New("error: this model is not linked, execute korm.AutoMigrate before")
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		query:      b.query,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprintf("%v", b.args...),
	}
	if useCache {
		if v, ok := cachesAllS.Get(c); ok {
			return v.([]T), nil
		}
	}
	wf := map[string]any{}
	if b.whereQuery != "" {
		if strings.Contains(b.whereQuery, ",") {
			sp := strings.Split(b.whereQuery, ",")
			if len(b.args) == len(sp) {
				for i, s := range sp {
					wf[strings.TrimSpace(s)] = b.args[i]
				}
			}
		} else {
			if len(b.args) == 1 {
				wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
			}
		}
	}

	if len(wf) == 0 {
		wf = nil
	}
	if b.ctx == nil {
		b.ctx = context.Background()
	}
	data, err := kmongodriver.Query[T](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), b.orderBys, b.database)
	if err != nil {
		return nil, err
	}
	if useCache {
		cachesAllS.Set(c, data)
	}
	return data, nil
}

func (b *Builder[T]) One() (T, error) {
	if b.database == "" {
		b.database = databases[0].Name
	}
	if b.tableName == "" {
		return *new(T), errors.New("error: this model is not linked, execute korm.AutoMigrate first")
	}
	c := dbCache{
		database:   b.database,
		table:      b.tableName,
		selected:   b.selected,
		statement:  b.statement,
		orderBys:   b.orderBys,
		whereQuery: b.whereQuery,
		query:      b.query,
		offset:     b.offset,
		limit:      b.limit,
		page:       b.page,
		args:       fmt.Sprintf("%v", b.args...),
	}
	if useCache {
		if v, ok := cachesOneS.Get(c); ok {
			return v.(T), nil
		}
	}
	wf := map[string]any{}
	if b.whereQuery != "" {
		if strings.Contains(b.whereQuery, ",") {
			sp := strings.Split(b.whereQuery, ",")
			if len(b.args) == len(sp) {
				for i, s := range sp {
					wf[strings.TrimSpace(s)] = b.args[i]
				}
			}
		} else {
			if len(b.args) == 1 {
				wf[strings.TrimSpace(b.whereQuery)] = b.args[0]
			}
		}
	}
	if len(wf) == 0 {
		wf = nil
	}
	if b.ctx == nil {
		b.ctx = context.Background()
	}
	data, err := kmongodriver.QueryOne[T](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), strings.ReplaceAll(b.orderBys, "ORDER BY", ""), b.database)
	if err != nil {
		return data, err
	}
	if useCache {
		cachesOneS.Set(c, data)
	}
	return data, nil
}

