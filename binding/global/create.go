package global

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"github.com/Chenpeng2013/case-gen/pkg/util"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type GlobalCreateBinding struct {
	Begin        bool
	GlobalCreate int
	spm_infos    []model.SPMInfo
}

func NewCreate(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	return &GlobalCreateBinding{
		GlobalCreate: 0,
		spm_infos:     s,
	}, nil
}

func (s *GlobalCreateBinding) Name() string {
	return "global_create_binding"
}

func (s *GlobalCreateBinding) Json() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (s *GlobalCreateBinding) Next() bool {
	if !s.Begin {
		s.Begin = true
		return true
	}

	total := 0
	for _, item := range s.spm_infos {
		total += len(item.OriginalSQLHints) + len(item.OtherOriginalSQLHints)
	}

	for s.GlobalCreate < (1 << total) - 1 {
		s.GlobalCreate++
		return true
	}
	return false
}

func (s *GlobalCreateBinding) Execute(ctx context.Context, conn *conn.Manager) error {
	oldsb := util.ShowBinding(conn, "session")

	index := 0
	db, err := conn.FuncValue("database()")
	if err != nil {
		panic(err)
	}
	
	for _, item := range s.spm_infos {

		if item.DataBase == db {
			// same database
			for i := 0; i < len(item.OriginalSQLHints); i++ {
				index++
				if s.GlobalCreate&(1<<i) == (1 << i) {
					oldBinding := util.ShowBinding(conn, "global")
					q := fmt.Sprintf("create global binding for %s using %s;", item.OriginalSQL, item.OriginalSQLHints[i])
					if _, err := conn.ExecContext(ctx, q); err != nil {
						return err
					}
					binding := util.ShowBinding(conn, "global")

					k := item.DataBase + ":" + strings.Replace(item.OriginalSQL, "10", "?", -1)
					sqlBind, ok := binding[k]
					if !ok {
						return fmt.Errorf("not exist original sql binding: %s", item.OriginalSQL)
					}

					bind, ok := sqlBind.Bindings[item.OriginalSQLHints[i]]
					if !ok {
						return fmt.Errorf("not exist bind sql: %s", item.OriginalSQLHints[i])
					}

					if bind.Status != "using" {
						return fmt.Errorf("unexpectd status: %s", bind.Status)
					}
					if bind.CreateTime > bind.UpdateTime {
						return fmt.Errorf("time error: %s %s", bind.CreateTime, bind.UpdateTime)
					}

					delete(oldBinding, k)
					delete(binding, k)
					if !(util.BindRecords(oldBinding)).Equal(binding) {
						return fmt.Errorf("other global binding changed")
					}
				}
			}
		} else {
			index += len(item.OriginalSQLHints)
		}

		for i := 0; i < len(item.OtherOriginalSQLHints); i++ {
			index++
			if s.GlobalCreate&(1<<i) == (1 << i) {
				oldBinding := util.ShowBinding(conn, "global")
				q := fmt.Sprintf("create global binding for %s using %s;", item.OtherOriginalSQL, item.OtherOriginalSQLHints[i])
				if _, err := conn.ExecContext(ctx, q); err != nil {
					return err
				}
				binding := util.ShowBinding(conn, "global")

				k := ":" + strings.Replace(item.OtherOriginalSQL, "10", "?", -1)
				sqlBind, ok := binding[k]
				if !ok {
					return fmt.Errorf("not exist original sql binding: %s", item.OtherOriginalSQL)
				}

				bind, ok := sqlBind.Bindings[item.OtherOriginalSQLHints[i]]
				if !ok {
					return fmt.Errorf("not exist bind sql: %s", item.OtherOriginalSQLHints[i])
				}

				if bind.Status != "using" {
					return fmt.Errorf("unexpectd status: %s", bind.Status)
				}
				if bind.CreateTime > bind.UpdateTime {
					return fmt.Errorf("time error: %s %s", bind.CreateTime, bind.UpdateTime)
				}

				delete(oldBinding, k)
				delete(binding, k)
				if !(util.BindRecords(oldBinding)).Equal(binding) {
					return fmt.Errorf("other global binding changed")
				}
			}
		}
	}
	newsb := util.ShowBinding(conn, "session")
	if !util.BindRecords(newsb).Equal(oldsb) {
		return fmt.Errorf("session binding been changed")
	}
	return nil
}
