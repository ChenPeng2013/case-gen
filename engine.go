package main

import (
	"context"
	"fmt"
	"github.com/Chenpeng2013/case-gen/binding/global"
	"github.com/Chenpeng2013/case-gen/binding/session"
	"github.com/Chenpeng2013/case-gen/database"
	"github.com/Chenpeng2013/case-gen/explain"
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"github.com/Chenpeng2013/case-gen/table"
	"github.com/Chenpeng2013/case-gen/variable"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Config struct {
	Name string
	Json string
}

var (
	gen  = make(map[string]func(ctx context.Context) (model.Interface, error))
	seqs = []Config{
		{Name: "spm_table"},

		{Name: "switch_database"},
		{Name: "bool_variable", Json: `{"Variable": "tidb_use_plan_baselines"}`},
		{Name: "session_create_binding"},
		{Name: "session_create_binding"},

		{Name: "global_create_binding"},
		{Name: "global_create_binding"},

		{Name: "bool_variable", Json: `{"Variable": "tidb_use_plan_baselines"}`},
		{Name: "explain"},

		{Name: "global_clean_binding"},
		{Name: "explain"},
	}
)

func main() {
	gen["explain"] = explain.New
	gen["session_create_binding"] = session.NewCreate
	gen["global_create_binding"] = global.NewCreate
	gen["global_clean_binding"] = global.NewClean
	gen["switch_database"] = database.New
	gen["spm_table"] = table.New
	gen["bool_variable"] = variable.New

	ctx := context.Background()

	if len(seqs[0].Json) != 0 {
		ctx = context.WithValue(ctx, "json", seqs[0].Json)
	}
	ctx = context.WithValue(ctx, "spm_infos", model.NewSPMInfos())
	v, err := gen[seqs[0].Name](ctx)
	if err != nil {
		panic(err)
	}

	s := []model.Interface{v}
	index := 0
	for len(s) != 0 {
		i := len(s) - 1
		for i >= 0 && !s[i].Next() {
			s = s[:len(s)-1]
			i = len(s) - 1
		}
		if i == -1 {
			break
		}
		for j := i + 1; j < len(seqs); j++ {
			if len(seqs[j].Json) != 0 {
				ctx = context.WithValue(ctx, "json", seqs[j].Json)
			} else {
				ctx = context.WithValue(ctx, "json", nil)
			}
			t, err := gen[seqs[j].Name](ctx)
			if err != nil {
				panic(err)
			}
			s = append(s, t)
			s[j].Next()
		}

		dsn := "root@tcp(127.0.0.1:4000)/"
		m, err := conn.New(dsn)
		if err != nil {
			panic(err)
		}

		log.Info("case:", zap.Int("index", index))
		for k := 0; k < len(s); k++ {
			if err := s[k].Execute(ctx, m); err != nil {
				panic(err)
			}
		}
		log.Info("passed", zap.Int("index", index))
		m.Close()

		index++
	}
	fmt.Println("all done")
}
