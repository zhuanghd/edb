package edb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"time"
	"unicode"
)

const TimeFormat = "2006-01-02 15:04:05"

type Database struct {
	underlay *sql.DB
}

type Time time.Time

func (t *Time) UnmarshalJSON(data []byte) (err error) {
	tt := string(data)
	tt = strings.Replace(tt, "T", " ", 1)
	tt = strings.Replace(tt, "Z", "", 1)
	now, err := time.ParseInLocation(`"`+TimeFormat+`"`, tt, time.Local)
	*t = Time(now)
	return
}

func (jt Time) MarshalJSON() ([]byte, error) {
	var stamp = fmt.Sprintf("\"%s\"", time.Time(jt).Format(TimeFormat))
	if strings.HasPrefix(stamp, "\"000") {
		return []byte("null"), nil
	}
	return []byte(stamp), nil
}

type Table struct {
	Name                      string
	Database                  *Database
	AutoCamelCaseToUnderscore bool
}

// MySQL 创建MySQL数据库对象
func MySQL(host string, port int, dbname string, username string, password string) *Database {
	ds := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8&parseTime=True", username, password, host, port, dbname)
	db, err := sql.Open("mysql", ds)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	return &Database{
		underlay: db,
	}
}

// SQLite 创建SQLite数据库对象
func SQLite(dbFile string) *Database {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	return &Database{
		underlay: db,
	}
}

// WithConnectionConfig 数据库连接配置
func (db *Database) WithConnectionConfig(connTimeoutSec int, connMaxOpen int, connMaxIdle int) *Database {
	// 最大连接时长
	db.underlay.SetConnMaxLifetime(time.Second * time.Duration(connTimeoutSec))
	// 最大连接数
	db.underlay.SetMaxOpenConns(connMaxOpen)
	// 空闲连接数
	db.underlay.SetMaxIdleConns(connMaxIdle)
	return db
}

// Table 获取表对象
func (db *Database) Table(name string) *Table {
	return &Table{
		Name:                      name,
		Database:                  db,
		AutoCamelCaseToUnderscore: true,
	}
}

// WithAutoCamelCaseToUnderscore 获取表对象
func (table *Table) WithAutoCamelCaseToUnderscore(isAuto bool) *Table {
	table.AutoCamelCaseToUnderscore = isAuto
	return table
}

// Execute 插入单条数据
func (db *Database) Execute(sql string, args ...any) error {
	_, err := db.underlay.Exec(sql, args...)
	return err
}

// InsertOne 插入单条数据
func (table *Table) InsertOne(record interface{}) error {
	mapping := createMappings(record)
	cols := make([]string, 0)
	ques := make([]string, 0)
	vars := make([]interface{}, 0)
	for k, v := range mapping {
		if table.AutoCamelCaseToUnderscore {
			cols = append(cols, camelCaseToUnderscore(k))
		} else {
			cols = append(cols, k)
		}
		ques = append(ques, "?")

		vars = append(vars, v)
	}
	qry := fmt.Sprintf("insert into %v (%v) values (%v)", table.Name, strings.Join(cols, ","), strings.Join(ques, ","))
	_, err := table.Database.underlay.Exec(qry, vars...)
	return err
}

// UpdateOne 插入单条数据
func (table *Table) UpdateOne(pk string, record interface{}) error {
	mapping := createMappings(record)
	pairs := make([]string, 0)
	vars := make([]interface{}, 0)

	for k, v := range mapping {
		col := k
		if table.AutoCamelCaseToUnderscore {
			col = camelCaseToUnderscore(k)
		}
		if col == pk {
			continue
		}
		pairs = append(pairs, col+"=?")
		vars = append(vars, v)
	}
	qry := "update " + table.Name + " set " + strings.Join(pairs, ",") + " where " + pk + " = ?"
	vars = append(vars, mapping[pk])
	_, err := table.Database.underlay.Exec(qry, vars...)
	return err
}

// Delete 删除数据
func (table *Table) Delete(param interface{}) int64 {
	mapping := createMappings(param)
	colnames := make([]string, 0)
	scannames := make([]string, 0)
	pairs := make([]string, 0)
	vars := make([]interface{}, 0)
	for k, v := range mapping {
		scannames = append(scannames, k)
		col := k
		if table.AutoCamelCaseToUnderscore {
			col = camelCaseToUnderscore(k)
		}
		colnames = append(colnames, col)
		if v == nil {
			continue
		}
		pairs = append(pairs, col+"=?")
		vars = append(vars, v)
	}
	qry := "delete from " + table.Name + " where " + strings.Join(pairs, " and ")
	result, err := table.Database.underlay.Exec(qry, vars...)
	if err != nil {
		return 0
	}
	count, _ := result.RowsAffected()
	return count
}

// Select 查询数据
func (table *Table) Select(param interface{}, arr any) error {
	mapping := createMappings(param)
	colnames := make([]string, 0)
	scannames := make([]string, 0)
	pairs := make([]string, 0)
	vars := make([]interface{}, 0)
	for k, v := range mapping {
		scannames = append(scannames, k)
		col := k
		if table.AutoCamelCaseToUnderscore {
			col = camelCaseToUnderscore(k)
		}
		colnames = append(colnames, col)
		if v == nil {
			continue
		}
		switch v.(type) {
		case string:
			str := v.(string)
			if strings.HasPrefix(str, "%") || strings.HasPrefix(str, "%") {
				pairs = append(pairs, col+" like ?")
			} else {
				pairs = append(pairs, col+"=?")
			}
		default:
			pairs = append(pairs, col+"=?")
		}
		vars = append(vars, v)
	}
	qry := "select * from " + table.Name + " where " + strings.Join(pairs, " and ")
	rows, err := table.Database.underlay.Query(qry, vars...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, arr)
}

func createMappings(record interface{}) map[string]interface{} {
	var mapping map[string]interface{}
	switch record.(type) {
	case nil:
		return nil
	case string:
		json.Unmarshal([]byte(record.(string)), &mapping)
	case map[string]interface{}:
		mapping = record.(map[string]interface{})
	default:
		data, _ := json.Marshal(record)
		_ = json.Unmarshal(data, &mapping)
	}
	return mapping
}

// 驼峰转下划线
func camelCaseToUnderscore(s string) string {
	var output []rune
	for i, r := range s {
		if i == 0 {
			output = append(output, unicode.ToLower(r))
		} else {
			if unicode.IsUpper(r) {
				output = append(output, '_')
			}

			output = append(output, unicode.ToLower(r))
		}
	}
	return string(output)
}

// InsertMany 保存多条数据
func (table *Table) InsertMany(records interface{}) int {
	all := createInterfaceArray(records)
	if all == nil || len(all) == 0 {
		return 0
	}
	counts := 0
	for _, record := range all {
		err := table.InsertOne(record)
		if err == nil {
			counts++
		}
	}
	return counts
}

// UpdateMany 更新多条数据
func (table *Table) UpdateMany(pk string, records interface{}) int {
	all := createInterfaceArray(records)
	if all == nil || len(all) == 0 {
		return 0
	}
	counts := 0
	for _, record := range all {
		err := table.UpdateOne(pk, record)
		if err == nil {
			counts++
		}
	}
	return counts
}

func createInterfaceArray(records interface{}) []interface{} {
	var all []interface{}
	switch records.(type) {
	case nil:
		return all
	case string:
		err := json.Unmarshal([]byte(records.(string)), &all)
		if err != nil {
			return all
		}
	default:
		data, _ := json.Marshal(records)
		_ = json.Unmarshal(data, &all)
	}
	return all
}
