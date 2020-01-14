package model

import (
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"context"
)

type Interface interface {
	Name() string
	Json() (string, error)
	Next() bool
	Execute(ctx context.Context, conn *conn.Manager) error
}

type SPMInfo struct {
	DataBase         string 
	TableSQL         string
	OriginalSQLHints []string
	OriginalSQL      string
	BindSQL          []string
	EvolveSQL         []string
	ExplainTemplate  []string

	OtherOriginalSQLHints []string
	OtherOriginalSQL 	string
	OtherBindSQL   		[]string
	OtherEvolveSQL      []string
	OtherExplainTemplate []string
}

func NewSPMInfos() []SPMInfo {
	t := SPMInfo{}
	t.DataBase = "test"
	t.TableSQL = "drop table if exists t; create table t(a int, index idx_a(a));"
	t.OriginalSQL = "select * from t where a > 10"
	t.OriginalSQLHints = []string{"select * from t use index(idx_a) where a > 10",
		"select * from t ignore index(idx_a) where a > 10"}
	t.BindSQL = []string{"select * from t use index(idx_a) where a > 10",
		"select * from t ignore index(idx_a) where a > 10"}
	t.ExplainTemplate = []string{`id	count	task	operator info
IndexReader[[s]]
└─IndexScan[[s]]
`,
		`id	count	task	operator info
TableReader[[s]]
└─Selection[[s]]
  └─TableScan[[s]]
`}
	t.EvolveSQL = []string{}

	t.OtherOriginalSQL = "select * from test.t where a > 10"
	t.OtherOriginalSQLHints = []string{"select * from test.t use index(idx_a) where a > 10",
		"select * from test.t ignore index(idx_a) where a > 10"}
	t.OtherBindSQL = []string{"select * from test.t use index(idx_a) where a > 10",
		"select * from test.t ignore index(idx_a) where a > 10"}
	t.OtherExplainTemplate = []string{`id	count	task	operator info
IndexReader[[s]]
└─IndexScan[[s]]
`,
		`id	count	task	operator info
TableReader[[s]]
└─Selection[[s]]
  └─TableScan[[s]]
`}
	t.OtherEvolveSQL = []string{}

	second := SPMInfo{}
	second.DataBase = "second"
	second.TableSQL = "drop table if exists t; create table t(a int, index idx_a(a));"
	second.OriginalSQL = "select * from t where a > 10"
	second.OriginalSQLHints = []string{"select * from t use index(idx_a) where a > 10",
		"select * from t ignore index(idx_a) where a > 10"}
	second.BindSQL = []string{"select * from t use index(idx_a) where a > 10",
		"select * from t ignore index(idx_a) where a > 10"}
	second.ExplainTemplate = []string{`id	count	task	operator info
IndexReader[[s]]
└─IndexScan[[s]]
`,
		`id	count	task	operator info
TableReader[[s]]
└─Selection[[s]]
  └─TableScan[[s]]
`}
	second.EvolveSQL = []string{}

	second.OtherOriginalSQL = "select * from second.t where a > 10"
	second.OtherOriginalSQLHints = []string{"select * from second.t use index(idx_a) where a > 10",
		"select * from second.t ignore index(idx_a) where a > 10"}
	second.OtherBindSQL = []string{"select * from second.t use index(idx_a) where a > 10",
		"select * from second.t ignore index(idx_a) where a > 10"}
	second.OtherExplainTemplate = []string{`id	count	task	operator info
IndexReader[[s]]
└─IndexScan[[s]]
`, 
`id	count	task	operator info
TableReader[[s]]
└─Selection[[s]]
  └─TableScan[[s]]
`}
	second.OtherEvolveSQL = []string{}
	return []SPMInfo{t, second}
}