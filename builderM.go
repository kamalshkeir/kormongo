package kormongo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kamalshkeir/klog"
	"github.com/kamalshkeir/kmongodriver"
)

type BuilderM struct {
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

func Table(tableName string) *BuilderM {
	return &BuilderM{
		tableName: tableName,
	}
}

func BuilderMap(tableName string) *BuilderM {
	return &BuilderM{
		tableName: tableName,
	}
}

func (b *BuilderM) Database(dbName string) *BuilderM {
	b.database = dbName
	return b
}

func (b *BuilderM) Select(columns ...string) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse .Table before .Select\n")
		return nil
	}
	s := []string{}
	s = append(s, columns...)
	b.selected = strings.Join(s, ",")
	b.order = append(b.order, "select")
	return b
}

func (b *BuilderM) Where(fieldsCommaSeparated string, args ...any) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse .Table before .Where\n")
		return nil
	}
	b.whereQuery = fieldsCommaSeparated
	b.args = append(b.args, args...)
	b.order = append(b.order, "where")
	return b
}

func (b *BuilderM) Limit(limit int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Limit\n")
		return nil
	}
	b.limit = limit
	b.order = append(b.order, "limit")
	return b
}

func (b *BuilderM) Page(pageNumber int) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Page\n")
		return nil
	}
	b.page = pageNumber
	b.order = append(b.order, "page")
	return b
}

func (b *BuilderM) OrderBy(fields ...string) *BuilderM {
	if b.database == "" {
		b.database = databases[0].Name
	}
	if _, ok := kmongodriver.MMongoDBS.Get(b.database); ok {
		b.orderBys = strings.Join(fields, ",")
		b.order = append(b.order, "order_by")
		return b
	}
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before OrderBy\n")
		return nil
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

func (b *BuilderM) Context(ctx context.Context) *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Context\n")
		return nil
	}
	b.ctx = ctx
	return b
}

func (b *BuilderM) Debug() *BuilderM {
	if b.tableName == "" {
		klog.Printf("rdUse db.Table before Debug\n")
		return nil
	}
	b.debug = true
	return b
}

func (b *BuilderM) All() ([]map[string]any, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
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
		if v, ok := cachesAllM.Get(c); ok {
			return v, nil
		}
	}
	if b.database == "" {
		b.database = databases[0].Name
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
	data, err := kmongodriver.Query[map[string]any](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), b.orderBys, b.database)
	if err != nil {
		return nil, err
	}
	if useCache {
		cachesAllM.Set(c, data)
	}
	return data, nil
}

func (b *BuilderM) One() (map[string]any, error) {
	if b.tableName == "" {
		return nil, errors.New("unable to find table, try db.Table before")
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
		if v, ok := cachesOneM.Get(c); ok {
			return v, nil
		}
	}
	if b.database == "" {
		b.database = databases[0].Name
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
	data, err := kmongodriver.QueryOne[map[string]any](b.ctx, b.tableName, b.selected, wf, int64(b.limit), int64(b.page), strings.ReplaceAll(b.orderBys, "ORDER BY", ""), b.database)
	if err != nil {
		return nil, err
	}
	if useCache {
		cachesOneM.Set(c, data)
	}
	return data, nil
}

// Insert usage: Insert("email, is_admin","example@mail.com",true)
func (b *BuilderM) Insert(fieldsCommaSeparated string, fields_values ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find table, try db.Table before")
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
	if err != nil {
		return 0, err
	}
	

	mmm := map[string]any{}
	sp := strings.Split(fieldsCommaSeparated, ",")
	for i, s := range sp {
		mmm[s] = fields_values[i]
	}
	if b.ctx == nil {
		b.ctx = context.Background()
	}
	err = kmongodriver.CreateRow(b.ctx, b.tableName, mmm, db.Name)
	if klog.CheckError(err) {
		return 0, err
	}
	return 1, nil
}

// Set usage: Set("email, is_admin","example@mail.com",true)
func (b *BuilderM) Set(fieldsCommaSeparated string, args ...any) (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try db.Table before")
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
	if err != nil {
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

func (b *BuilderM) Delete() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.AutoMigrate before")
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
	if err != nil {
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

func (b *BuilderM) Drop() (int, error) {
	if b.tableName == "" {
		return 0, errors.New("unable to find model, try korm.LinkModel before Update")
	}
	if b.database == "" {
		b.database = databases[0].Name
	}
	if useCache {
		cachebus.Publish(CACHE_TOPIC, map[string]any{
			"type":     "drop",
			"table":    b.tableName,
			"database": b.database,
		})
	}
	db, err := GetMemoryDatabase(b.database)
	if err != nil {
		return 0, err
	}

	if b.ctx == nil {
		b.ctx = context.Background()
	}
	kmongodriver.DropTable(b.ctx, b.tableName, db.Name)
	return 1, nil
}
