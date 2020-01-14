package variable

import (
	"github.com/Chenpeng2013/case-gen/model"
	"github.com/Chenpeng2013/case-gen/pkg/conn"
	"context"
	"encoding/json"
	"fmt"
)

type BoolVariable struct {
	begin          bool
	On 			   bool	  `json:"on"`
	Variable       string `json:"variable"`
}

func (v *BoolVariable) Name() string {
	return "bool_variable"
}

func New(ctx context.Context) (model.Interface, error) {
	if ctx.Value("json") == nil {
		return nil, fmt.Errorf("init session bool variable not exist json config")
	}

	s := ctx.Value("json").(string)
	spmVar := BoolVariable{}
	if err := json.Unmarshal([]byte(s), &spmVar); err != nil {
		return nil, err
	}
	if len(spmVar.Variable) == 0 {
		return nil, fmt.Errorf("init session bool variable not exist variable name, config:" + s)
	}
	return &spmVar, nil
}

func (v *BoolVariable) Json() (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (v *BoolVariable) Next() bool {
	if !v.begin {
		v.begin = true
		return true
	}
	
	for !v.On {
		v.On = !v.On
		return true
	}
	
	return false
}

func (v *BoolVariable) Execute(ctx context.Context, conn *conn.Manager) error {
	s := "set " + v.Variable + "="
	if v.On {
		s += "on;"
	} else {
		s += "off;"
	}
	
	if _, err := conn.ExecContext(context.Background(), s); err != nil {
		return err
	}
	
	return nil
}
