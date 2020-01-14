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

type GlobalDropBinding struct {
	Begin      bool
	GlobalDrop int
	spm_infos  []model.SPMInfo
}

func NewDrop(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	return &GlobalDropBinding{
		GlobalDrop: 0,
		spm_infos:  s,
	}, nil
}

func (s *GlobalDropBinding) Name() string {
	return "global_drop_binding"
}

func (s *GlobalDropBinding) Json() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (s *GlobalDropBinding) Next() bool {
	if !s.Begin {
		s.Begin = true
		return true
	}

	total := 0
	for _, item := range s.spm_infos {
		total += len(item.OriginalSQLHints) + len(item.OtherOriginalSQLHints)
	}

	for s.GlobalDrop < (1<<total)-1 {
		s.GlobalDrop++
		return true
	}
	return false
}

func (s *GlobalDropBinding) Execute(ctx context.Context, conn *conn.Manager) error {
	osbinding := util.ShowBinding(conn, "session")
	index := 0
	db, err := conn.FuncValue("database()")
	if err != nil {
		panic(err)
	}

	for _, item := range s.spm_infos {
		if db == item.DataBase {
			for i := 0; i < len(item.OriginalSQLHints); i++ {
				index++
				if s.GlobalDrop&(1<<i) == (1 << i) {
					q := fmt.Sprintf("drop global binding for %s using %s;", item.OriginalSQL, item.OriginalSQLHints[i])
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
				}
			}
		} else {
			index += len(item.OriginalSQLHints)
		}

		for i := 0; i < len(item.OtherOriginalSQLHints); i++ {
			index++
			if s.GlobalDrop&(1<<i) == (1 << i) {
				q := fmt.Sprintf("drop global binding for %s using %s;", item.OtherOriginalSQL, item.OtherOriginalSQLHints[i])
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
			}
		}
	}

	nsbinding := util.ShowBinding(conn, "session")
	if !util.BindRecords(osbinding).Equal(nsbinding) {
		return fmt.Errorf("session binding been changed")
	}
	return nil
}
