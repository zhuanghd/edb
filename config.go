package edb

import "fmt"

type KvConfig struct {
	db        *Database
	tableName string
	keyCol    string
	valueCol  string
}

func UseConfig(db *Database, table string) *KvConfig {
	cfg := &KvConfig{
		db:        db,
		tableName: table,
		keyCol:    "property",
		valueCol:  "value",
	}
	query := fmt.Sprintf("create table %v (%v varchar(4000) primary key, %v varchar(4000))", cfg.tableName, cfg.keyCol, cfg.valueCol)
	_, _ = cfg.db.underlay.Exec(query)
	return cfg
}

// Get 获取配置项的值
func (cfg *KvConfig) Get(key string) (string, error) {
	query := fmt.Sprintf("select %v from %v where %v=?", cfg.valueCol, cfg.tableName, cfg.keyCol)
	rows, err := cfg.db.underlay.Query(query, key)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ret string
	if rows.Next() {
		err = rows.Scan(&ret)
		if err != nil {
			return "", err
		}
	}
	return ret, nil
}

// Set 设置配置项
func (cfg *KvConfig) Set(key string, value string) error {
	_ = cfg.Remove(key)
	query := fmt.Sprintf("insert into %v(%v,%v) values(?,?)", cfg.tableName, cfg.keyCol, cfg.valueCol)
	_, err := cfg.db.underlay.Exec(query, key, value)
	if err != nil {
		return err
	}
	return nil
}

// Remove 删除配置项
func (cfg *KvConfig) Remove(key string) error {
	query := fmt.Sprintf("delete from %v where %v=?", cfg.tableName, cfg.keyCol)
	_, err := cfg.db.underlay.Exec(query, key)
	if err != nil {
		return err
	}
	return nil
}
