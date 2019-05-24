package dao

import (
	"bytes"
	"io"
	"text/template"
)

const (
	daoCode = `
	//GetOne gets one record from table {{.TableName}} by condition "where"
	func (*{{.StructName}})GetOne(db *sql.DB, where map[string]interface{}) (*{{.StructName}}, error) {
		if nil == db {
			return nil, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, nil)
		if nil != err {
			return nil, err
		}
		row,err := db.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res *{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//GetOne gets one record from table {{.TableName}} by condition "where" in Tx
	func (*{{.StructName}})GetOneTx(Tx *sql.Tx, where map[string]interface{}) (*{{.StructName}}, error) {
		if nil == Tx {
			return nil, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, nil)
		if nil != err {
			return nil, err
		}
		row,err := Tx.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res *{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//GetMulti gets multiple records from table {{.TableName}} by condition "where"
	func (*{{.StructName}})GetMulti(db *sql.DB, where map[string]interface{}) ([]*{{.StructName}}, error) {
		if nil == db {
			return nil, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, nil)
		if nil != err {
			return nil, err
		}
		row,err := db.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res []*{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//GetMulti gets multiple records from table {{.TableName}} by condition "where" in Tx
	func (*{{.StructName}})GetMultiTx(Tx *sql.Tx, where map[string]interface{}) ([]*{{.StructName}}, error) {
		if nil == Tx {
			return nil, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildSelect("{{.TableName}}", where, nil)
		if nil != err {
			return nil, err
		}
		row,err := Tx.Query(cond, vals...)
		if nil != err || nil == row {
			return nil, err
		}
		defer row.Close()
		var res []*{{.StructName}}
		err = scanner.Scan(row, &res)
		return res,err
	}

	//Insert inserts an array of data into table {{.TableName}}
	func (*{{.StructName}})Insert(db *sql.DB, data []map[string]interface{}) (int64, error) {
		if nil == db {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond, vals, err := builder.BuildInsert("{{.TableName}}", data)
		if nil != err {
			return 0, err
		}
		result,err := db.Exec(cond, vals...)
		if nil != err || nil == result {
			return 0, err
		}
		return result.LastInsertId()
	}

	//Insert inserts an array of data into table {{.TableName}} in Tx
	func (*{{.StructName}})InsertTx(Tx *sql.Tx, data []map[string]interface{}) (int64, error) {
		if nil == Tx {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond, vals, err := builder.BuildInsert("{{.TableName}}", data)
		if nil != err {
			return 0, err
		}
		result,err := Tx.Exec(cond, vals...)
		if nil != err || nil == result {
			return 0, err
		}
		return result.LastInsertId()
	}

	//Update updates the table {{.TableName}}
	func (*{{.StructName}})Update(db *sql.DB, where,data map[string]interface{}) (int64, error) {
		if nil == db {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildUpdate("{{.TableName}}", where, data)
		if nil != err {
			return 0, err
		}
		result,err := db.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}

	//Update updates the table {{.TableName}} in Tx
	func (*{{.StructName}})UpdateTx(Tx *sql.Tx, where,data map[string]interface{}) (int64, error) {
		if nil == Tx {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildUpdate("{{.TableName}}", where, data)
		if nil != err {
			return 0, err
		}
		result,err := Tx.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}

	// Delete deletes matched records in {{.TableName}}
	func (*{{.StructName}})Delete(db *sql.DB, where map[string]interface{}) (int64, error) {
		if nil == db {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildDelete("{{.TableName}}", where)
		if nil != err {
			return 0, err
		}
		result,err := db.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}

	// Delete deletes matched records in {{.TableName}} in Tx
	func (*{{.StructName}})DeleteTx(Tx *sql.Tx, where map[string]interface{}) (int64, error) {
		if nil == Tx {
			return 0, errors.New("sql.DB object couldn't be nil")
		}
		cond,vals,err := builder.BuildDelete("{{.TableName}}", where)
		if nil != err {
			return 0, err
		}
		result,err := Tx.Exec(cond, vals...)
		if nil != err {
			return 0, err
		}
		return result.RowsAffected()
	}
	`
)

type fillData struct {
	StructName string
	TableName  string
}

// GenerateDao generates Dao code
func GenerateDao(tableName, structName string) (io.Reader, error) {
	var buff bytes.Buffer
	err := template.Must(template.New("dao").Parse(daoCode)).Execute(&buff, fillData{
		StructName: structName,
		TableName:  tableName,
	})
	if nil != err {
		return nil, err
	}
	return &buff, nil
}
