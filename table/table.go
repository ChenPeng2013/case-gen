package table

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"context"
	"encoding/json"
	"fmt"
)

type SPMTable struct {
	begin 	  bool
	spm_infos []model.SPMInfo
}

func New(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	t := SPMTable{
		spm_infos: s,
	}
	return &t, nil
}

func (t *SPMTable) Name() string {
	return "spm_table"
}

func (t *SPMTable) Json() (string, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	
	return string(data), nil
}

func (t *SPMTable) Next() bool {
	if !t.begin {
		t.begin = true
		return true
	}
	
	return false
}

func (t *SPMTable) Execute(ctx context.Context, conn *conn.Manager) error {
	for _, item := range t.spm_infos {
		raw := fmt.Sprintf(`create database if not exists %s; use %s;`, item.DataBase, item.DataBase)
		if _, err := conn.ExecContext(context.Background(), raw); err != nil {
			return err
		}
		if _, err := conn.ExecContext(context.Background(), item.TableSQL); err != nil {
			return err
		}
	}
	return nil
}
