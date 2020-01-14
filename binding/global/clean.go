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

type GlobalCleanBinding struct {
	Begin        bool
}

func NewClean(ctx context.Context) (model.Interface, error) {
	return &GlobalCleanBinding{
	}, nil
}

func (s *GlobalCleanBinding) Name() string {
	return "global_clean_binding"
}

func (s *GlobalCleanBinding) Json() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (s *GlobalCleanBinding) Next() bool {
	if !s.Begin {
		s.Begin = true
		return true
	}
	
	return false
}

func (s *GlobalCleanBinding) Execute(ctx context.Context, conn *conn.Manager) error {
	gb := util.ShowBinding(conn, "global")
	for original, v := range gb {
		currentDatabase, err := conn.FuncValue("database()")
		if err != nil {
			return err
		}
		if currentDatabase != v.Db && len(v.Db) != 0{
			if _, err := conn.ExecContext(ctx, "use " + v.Db); err != nil {
				return err
			}
		}
		original = strings.TrimPrefix(original, v.Db + ":")
		query := "drop global binding for " + strings.Replace(original, "?", "-1", -1)
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	oldgb := util.ShowBinding(conn, "global")
	if len(oldgb) != 0 {
		for k, v := range oldgb {
			fmt.Println(k)
			fmt.Println(v)
		}
		return fmt.Errorf("clean all binding failed")
	}
	return nil
}
