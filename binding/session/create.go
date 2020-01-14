package session

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"github.com/Chenpeng2013/case-gen/pkg/util"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type SessionCreateBinding struct {
	Begin 		   bool
	SessionCreate  int
	spm_infos   	[]model.SPMInfo
}

func NewCreate(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	return &SessionCreateBinding{
		SessionCreate: 0,
		spm_infos:     s,
	}, nil
}

func (s *SessionCreateBinding) Name() string {
	return "session_create_binding"
}

func (s *SessionCreateBinding) Json() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	
	return string(data), nil
}

func (s *SessionCreateBinding) Next() bool {
	if !s.Begin {
		s.Begin = true
		return true
	}
	
	total := 0
	for _, item := range s.spm_infos {
		total += len(item.OriginalSQLHints) + len(item.OtherOriginalSQLHints)
	}
	
	for s.SessionCreate < (1 << total) - 1 {
		s.SessionCreate++
		return true
	}
	return false
}

func (s *SessionCreateBinding) Execute(ctx context.Context, conn *conn.Manager) error {
	ogbinding := util.ShowBinding(conn, "global")
	
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
				if s.SessionCreate&(1<<index) == (1 << index) {
					oldbinding := util.ShowBinding(conn, "session")
					q := fmt.Sprintf("create session binding for %s using %s;", item.OriginalSQL, item.OriginalSQLHints[i])
					if _, err := conn.ExecContext(ctx, q); err != nil {
						return err
					}
					binding := util.ShowBinding(conn, "session")

					k := ":" + strings.Replace(item.OriginalSQL, "10", "?", -1)
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

					delete(oldbinding, k)
					delete(binding, k)
					if !util.BindRecords(oldbinding).Equal(binding) {
						return fmt.Errorf("other session binding been changed")
					}
				}
			}
		} else {
			index += len(item.OriginalSQLHints)
		}
		
		for i := 0; i < len(item.OtherOriginalSQLHints); i++ {
			index++
			if s.SessionCreate&(1<<index) == (1 << index) {
				oldbinding := util.ShowBinding(conn, "session")
				q := fmt.Sprintf("create session binding for %s using %s;", item.OtherOriginalSQL, item.OtherOriginalSQLHints[i])
				if _, err := conn.ExecContext(ctx, q); err != nil {
					return err
				}
				binding := util.ShowBinding(conn, "session")

				k := item.DataBase + ":" + strings.Replace(item.OtherOriginalSQL, "10", "?", -1)
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

				delete(oldbinding, k)
				delete(binding, k)
				if !util.BindRecords(oldbinding).Equal(binding) {
					return fmt.Errorf("other session binding been changed")
				}
			}
		}
	}
	
	ngbinding := util.ShowBinding(conn, "global")
	if !util.BindRecords(ogbinding).Equal(ngbinding) {
		return fmt.Errorf("global binding been changed")
	}
	return nil
}


