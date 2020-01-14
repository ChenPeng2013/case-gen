package util

import (
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"context"
	"database/sql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"strings"
)

// Binding stores the basic bind hint info.
type Binding struct {
	BindSQL string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. using: Binding is in the normal active mode.
	Status     string
	CreateTime string
	UpdateTime string
	Charset    string
	Collation  string
}

func (b * Binding) Equal(o *Binding) bool {
	log.Info("binding ", zap.Any("old", *b), zap.Any("new", *o))
	if b.BindSQL != o.BindSQL {
		return false
	}
	if b.Status != o.Status {
		return false
	}
	
	if b.CreateTime != o.CreateTime {
		return false
	}
	if b.UpdateTime != o.UpdateTime {
		return false
	}
	if b.Charset != o.Charset {
		return false
	}
	if b.Charset != o.Charset {
		return false
	}
	return true
}

// BindRecord represents a sql bind record retrieved from the storage.
type BindRecord struct {
	OriginalSQL string
	Db          string

	// For checking uniqueness.
	Bindings map[string]Binding
}

func (b *BindRecord) Equal(o *BindRecord) bool {
	log.Info("bind record", zap.Any("old", *b), zap.Any("new", *o))
	if b.Db != o.Db {
		return false
	}
	if b.OriginalSQL != o.OriginalSQL {
		return false
	}
	
	if len(b.Bindings) != len(o.Bindings) {
		return false
	}
	for ob, obv := range o.Bindings {
		nbv, ok := o.Bindings[ob]
		if !ok {
			return false
		}
		if !obv.Equal(&nbv) {
			return false
		}
	}
	return true
}


func parseShowBindings(rows *sql.Rows, handleDuplicate bool) map[string]*BindRecord {
	var originalSQL, bindSQL, db, status, createTime, updateTime, charset, collation string
	bindRecords := make(map[string]*BindRecord)
	for rows.Next() {
		err := rows.Scan(&originalSQL, &bindSQL, &db, &status, &createTime, &updateTime, &charset, &collation)
		if err != nil {
			panic(err)
		}
		
		originalSQL := strings.Replace(originalSQL, " . ", ".", -1)
		binding := Binding{
			BindSQL:    bindSQL,
			Status:     status,
			CreateTime: createTime,
			UpdateTime: updateTime,
			Charset:    charset,
			Collation:  collation,
		}
		key := db + ":" + originalSQL
		record, ok := bindRecords[key]
		if !ok {
			bindings := make(map[string]Binding)
			bindings[bindSQL] = binding
			bindRecords[key] = &BindRecord{
				OriginalSQL: originalSQL,
				Db:          db,
				Bindings:    bindings,
			}
			continue
		}
		if oldBinding, ok := record.Bindings[bindSQL]; ok {
			// Raise errors
			if !handleDuplicate {
				panic(bindSQL)
			}
			if strings.Compare(oldBinding.UpdateTime, binding.UpdateTime) <= 0 {
				record.Bindings[bindSQL] = binding
				continue
			}
		}
		record.Bindings[bindSQL] = binding
	}
	return bindRecords
}

func ShowBinding(conn *conn.Manager, from string) map[string]*BindRecord {
	s := ""
	d := false
	if from == "session" {
		s = "show session bindings"
		d = false
	} else if from == "global" {
		s = "show global bindings"
		d = false
	} else if from == "bind_info" {
		s = "select * from bind_info"
		d = true
	} else {
		panic(from)
	}

	rows, err := conn.QueryContext(context.Background(), s)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	return parseShowBindings(rows, d)
}

type BindRecords map[string]*BindRecord

func (b BindRecords) Equal(other map[string]*BindRecord) bool {
	ob := (map[string]*BindRecord)(b)
	log.Info("bindRecords", zap.Any("old", ob), zap.Any("new", other))
	
	for o, ov := range ob {
		nv, ok := other[o]
		if !ok {
			log.Error("other not exist original sql", zap.String("original", o))
			return false
		}
		if !ov.Equal(nv) {
			return false
		}
	}
	for n, _ := range other {
		if _, ok := ob[n]; !ok {
			log.Error("not exist original sql", zap.String("original", n))
			return false
		}
	}
	return true
}