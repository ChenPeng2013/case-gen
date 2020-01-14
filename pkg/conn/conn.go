package conn

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

type Manager struct {
	cur  string
	// key=dsn
	Dbs map[string]*sql.DB
	pool map[string]*sql.Conn
}

func New(dsn string) (*Manager, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	
	c, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	
	n := &Manager{
		cur:      "",
		Dbs:      map[string]*sql.DB{dsn: db},
		pool:     map[string]*sql.Conn{"": c},
	}
	return n, nil
}


func (m *Manager) Close() error {
	var errStr string
	for _, c := range m.pool {
		if e := c.Close(); e != nil {
			errStr = errStr + "," + e.Error() 
		}
	}
	for _, db := range m.Dbs {
		if e := db.Close(); e != nil {
			errStr = errStr + "," + e.Error()
		}
	}
	
	if len(errStr) == 0 {
		return nil
	}
	return fmt.Errorf(errStr)
}

func (m *Manager) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	log.Info(query)
	c, ok := m.pool[m.cur]
	if !ok {
		return nil, fmt.Errorf("not exist %s connection", m.cur)
	}
	return c.QueryContext(ctx, query)
}

func (m *Manager) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) { 
	log.Info(query)
	c, ok := m.pool[m.cur]
	if !ok {
		return nil, fmt.Errorf("not exist %s connection", m.cur)
	}
	
	return c.ExecContext(ctx, query)
}

func (m *Manager) VariableBoolValue(name string) (bool, error) {
	q := fmt.Sprintf("select @@%s;", name)
	r, err := m.QueryContext(context.Background(), q)
	if err != nil {
		return false, err
	}
	defer r.Close()
	
	v := ""
	for r.Next() {
		if err := r.Scan(&v); err != nil {
			return false, err
		}
	}
	
	if v == "on" {
		return true, nil
	}
	if v == "off" {
		return false, nil
	}
	
	log.Panic("unexpected valued", zap.String(name, v))
	return false, nil
}

func (m *Manager) FuncValue(f string) (string, error) {
	q := fmt.Sprintf("select %s;", f)
	r, err := m.QueryContext(context.Background(), q)
	if err != nil {
		return "", err
	}
	defer r.Close()

	v := ""
	bs := []byte{}
	for r.Next() {
		if err := r.Scan(&bs); err != nil {
			return "", err
		}
		if bs != nil {
			v = string(bs)
		}
	}
	return v, nil
}


func (m *Manager) QueryResult(ctx context.Context, query string) (string, error) {
	rows, err := m.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	br, err := dumpToByteRows(rows)
	if err != nil {
		return "", err
	}
	
	return br.convertToString(""), nil
}

func trimSQL(sql string) string {
	// Trim space.
	sql = strings.TrimSpace(sql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(sql, "/*") {
		i := strings.Index(sql, "*/")
		if i != -1 && i < len(sql)+1 {
			sql = sql[i+2:]
			sql = strings.TrimSpace(sql)
			continue
		}
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	return strings.TrimLeft(sql, "( ")
}

// IsQuery checks if a sql statement is a query statement.
func IsQuery(sql string) bool {
	queryStmtTable := []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}
	sqlText := strings.ToLower(trimSQL(sql))
	for _, key := range queryStmtTable {
		if strings.HasPrefix(sqlText, key) {
			return true
		}
	}
	return false
}

func (rows *byteRows) convertToString(fmt string) string {
	switch fmt {
	case "vertical":
		var buf bytes.Buffer
		for i, row := range rows.data {
			buf.WriteString("# ROW:" + strconv.Itoa(i+1) + "\n")
			for j, col := range rows.cols {
				buf.WriteString(col)
				buf.WriteByte('\t')
				buf.Write(row.data[j])
				buf.WriteByte('\n')
			}
		}
		return buf.String()
	default:
		res := strings.Join(rows.cols, "\t")
		for _, row := range rows.data {
			line := ""
			for _, data := range row.data {
				col := string(data)
				if data == nil {
					col = "NULL"
				}
				if len(line) > 0 {
					line = line + "\t"
				}
				line = line + col
			}
			res = res + "\n" + line
		}
		return res + "\n"
	}
}

type byteRow struct {
	data [][]byte
}

type byteRows struct {
	cols []string
	data []byteRow
}

func (rows *byteRows) Len() int {
	return len(rows.data)
}

func (rows *byteRows) Less(i, j int) bool {
	r1 := rows.data[i]
	r2 := rows.data[j]
	for i := 0; i < len(r1.data); i++ {
		res := bytes.Compare(r1.data[i], r2.data[i])
		switch res {
		case -1:
			return true
		case 1:
			return false
		}
	}
	return false
}

func (rows *byteRows) Swap(i, j int) {
	rows.data[i], rows.data[j] = rows.data[j], rows.data[i]
}

func dumpToByteRows(rows *sql.Rows) (*byteRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	data := make([]byteRow, 0, 8)
	args := make([]interface{}, len(cols))
	for rows.Next() {
		tmp := make([][]byte, len(cols))
		for i := 0; i < len(args); i++ {
			args[i] = &tmp[i]
		}
		err := rows.Scan(args...)
		if err != nil {
			return nil, err
		}

		data = append(data, byteRow{tmp})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return &byteRows{cols: cols, data: data}, nil
}
