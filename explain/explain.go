package explain

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"github.com/Chenpeng2013/case-gen/pkg/util"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/qa/once/common/checkstep/template"
	"strings"
)

type Explain struct {
	Begin bool
	spm_infos    []model.SPMInfo
}

func New(ctx context.Context) (model.Interface, error) {
	s := ctx.Value("spm_infos").([]model.SPMInfo)
	return &Explain{
		spm_infos:     s,
	}, nil
}

func (s *Explain) Name() string {
	return "explain"
}

func (s *Explain) Json() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	
	return string(data), nil
}

func (s *Explain) Next() bool {
	if !s.Begin {
		s.Begin = true
		return true
	}
	return false
}

func (s *Explain) Execute(ctx context.Context, conn *conn.Manager) error {
	b := util.ShowBinding(conn, "session")
	gb := util.ShowBinding(conn, "global")
	
	on, err := conn.VariableBoolValue("tidb_use_plan_baselines")
	if err != nil {
		return err
	}

	db, err := conn.FuncValue("database()")
	if err != nil {
		panic(err)
	}
	
	for _, item := range s.spm_infos {
		for i := 0; i < len(item.OriginalSQLHints); i++ {
			q := "explain " + item.OriginalSQLHints[i]
			result, err := conn.QueryResult(ctx, q)
			if err != nil {
				return err
			}

			temp := item.ExplainTemplate[i]

			k := item.DataBase + ":" + strings.Replace(item.OriginalSQL, "10", "?", -1)
			if on {
				if binding, ok := b[k]; ok {
					// exist on session binding
					if binding.Db == db {
						for k, _ := range binding.Bindings {
							if k == item.OriginalSQLHints[i] {
								temp = item.ExplainTemplate[i]
								break
							}
							for j := 0; j < len(item.OriginalSQLHints); j++ {
								if item.OriginalSQLHints[j] == k {
									temp = item.ExplainTemplate[j]
									break
								}
							}
						}
					}
				} else if gbinding, ok := gb[k]; ok {
					// exist on global binding
					for k, _ := range gbinding.Bindings {
						if k == item.OriginalSQLHints[i] {
							temp = item.ExplainTemplate[i]
							break
						}
						for j := 0; j < len(item.OriginalSQLHints); j++ {
							if item.OriginalSQLHints[j] == k {
								temp = item.ExplainTemplate[j]
								break
							}
						}
					}
				}
				
				// current database exist same sql binding， user current database template
				if item.DataBase != db {
					k := db + ":" + strings.Replace(item.OriginalSQL, "10", "?", -1)
					
					var other *model.SPMInfo
					for _, o := range s.spm_infos {
						if o.DataBase == db {
							other = &o
							break
						}
					}
					if other != nil {
						if binding, ok := b[k]; ok {
							// exist on session binding
							if binding.Db == db {
								for k, _ := range binding.Bindings {
									if k == other.OriginalSQLHints[i] {
										temp = other.ExplainTemplate[i]
										break
									}
									for j := 0; j < len(other.OriginalSQLHints); j++ {
										if other.OriginalSQLHints[j] == k {
											temp = other.ExplainTemplate[j]
											break
										}
									}
								}
							}
						} else if gbinding, ok := gb[k]; ok {
							// exist on global binding
							for k, _ := range gbinding.Bindings {
								if k == other.OriginalSQLHints[i] {
									temp = other.ExplainTemplate[i]
									break
								}
								for j := 0; j < len(other.OriginalSQLHints); j++ {
									if other.OriginalSQLHints[j] == k {
										temp = other.ExplainTemplate[j]
										break
									}
								}
							}
						}
					}
				}
			}
			if !template.Compare(result, temp) {
				panic(fmt.Sprintf("%s %s", result, temp))
			}
		}
	}
	return nil
}