package database

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"context"
	"encoding/json"
	"fmt"
)

type SwitchDatabase struct {
	begin          bool
	spm_infos      []model.SPMInfo
	spm_index      int
}

func (v *SwitchDatabase) Name() string {
	return "switch_database"
}

func New(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	d := SwitchDatabase{
		spm_infos: s,
	}
	return &d, nil
}

func (v *SwitchDatabase) Json() (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (v *SwitchDatabase) Next() bool {
	if !v.begin {
		v.begin = true
		return true
	}
	
	if v.spm_index < len(v.spm_infos) {
		v.spm_index++
		return true
	}
	
	return false
}

func (v *SwitchDatabase) Execute(ctx context.Context, conn *conn.Manager) error {
	var name string
	if v.spm_index < len(v.spm_infos) {
		name = v.spm_infos[v.spm_index].DataBase
		if _, err := conn.ExecContext(ctx, "use " + name); err != nil {
			return err
		}
		return nil
	}
	
	if v.spm_index == len(v.spm_infos) {
		name = "none_database"
	}
	s := fmt.Sprintf("create database if not exists %s; use %s; drop database %s;", name, name, name)
	if _, err := conn.ExecContext(ctx, s); err != nil {
		return err
	}
	
	return nil
}
