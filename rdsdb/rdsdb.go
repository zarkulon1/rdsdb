package rdsdb

import (
	data "database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
	"github.com/zarkulon1/fargomaths/trn"
)

/* adding to have file change */

type RdsDb struct {
	*data.DB
}

func ConnectRaw(database, connect string) (*RdsDb, error) {
	var err error
	var result RdsDb
	result.DB, err = data.Open(database, connect)
	return &result, err
}

// mysql make a connection wall params exposed
func ConnectFull(user, passwd, host, db string, port int) (*RdsDb, error) {
	connect := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, passwd, host, port, db)
	var result RdsDb
	var err error
	result.DB, err = data.Open("mysql", connect)
	return &result, err
}

// simple single arg connect
func Connect(ip string) (*RdsDb, error) {
	connect := fmt.Sprintf("rds:rds@tcp(%s:3306)/rds", ip)
	var result RdsDb
	var err error
	result.DB, err = data.Open("mysql", connect)
	result.CommonSettings()
	return &result, err
}

// connection pooling is the default
func (db *RdsDb) CommonSettings() {
	db.SetConnMaxLifetime(time.Minute * 15)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(4)
}

// if you want to not use connection pooling
func (db *RdsDb) SingleConn() {
	db.SetMaxOpenConns(1)
}

func (db *RdsDb) Ping() bool {
	return db.Ping()
}

func (db *RdsDb) GetString(otherwise, format string, args ...interface{}) string {
	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query)
	if err != nil {
		trn.Alert("failed [%s] (%s)", err.Error(), query)
		return otherwise
	}
	defer rows.Close()

	for rows.Next() {
		var result string
		err := rows.Scan(&result)
		if err != nil {
			trn.Alert("scan failed [%s]", err.Error())
			return otherwise
		}
		return result
	}
	return otherwise
}
func (db *RdsDb) GetInt(otherwise int, format string, args ...interface{}) int {
	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query)
	if err != nil {
		trn.Alert("failed [%s] (%s)", err.Error(), query)
		return otherwise
	}
	defer rows.Close()
	for rows.Next() {
		var result int
		err := rows.Scan(&result)
		if err != nil {
			trn.Alert("scan failed [%s]", err.Error())
			return otherwise
		}
		return result
	}
	return otherwise
}

func (db *RdsDb) GetControl(zone, name, otherwise string) string {
	hostname, _ := os.Hostname()
	return db.GetString(otherwise,
		"SELECT value FROM controls WHERE "+
			"host='%s' zone='%s' AND name='%s'",
		hostname, zone, name)
}
func (db *RdsDb) GetControlMap(zone string) map[string]string {
	hostname, _ := os.Hostname()
	query := fmt.Sprintf("SELECT name,value FROM controls WHERE "+
		"host='%s' AND zone='%s'", hostname, zone)
	rows, err := db.Query(query)
	defer rows.Close()
	if err != nil {
		trn.Alert("GetControlMap failed [%s]", err.Error())
		return nil
	}
	result := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			trn.Alert("GetControlMap Scan fail [%s]", err.Error())
			return nil
		}
		result[name] = value
	}
	return result
}

func (db *RdsDb) GetRuntime(name string) string {
	return db.GetString("", "SELECT value FROM runtime WHERE name='%s'", name)
}

func (db *RdsDb) SetRuntime(name, value string) error {
	stmt, err := db.Prepare("UPDATE runtime SET value=? WHERE name=?")
	if err != nil {
		trn.Alert("query error [%s]", err.Error())
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(value, name)
	if err != nil {
		trn.Alert("query error [%s]", err.Error())
		return err
	}
	return nil
}

func (db *RdsDb) FormattedExec(format string, args ...interface{}) (data.Result, error) {
	query := fmt.Sprintf(format, args...)
	return db.Exec(query)
}
func (db *RdsDb) FormattedQuery(format string, args ...interface{}) (*data.Rows, error) {
	query := fmt.Sprintf(format, args...)
	return db.Query(query)
}

const (
	ARRAY_INIT = 32
)

func (db *RdsDb) GetValueArray(format string, args ...interface{}) ([]string, error) {
	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query)
	if err != nil {
		trn.Alert("GetValueArray failed [%s] (%s)", err.Error(), query)
		return nil, err
	}
	defer rows.Close()
	size := ARRAY_INIT
	var count int
	result := make([]string, 0, size)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			trn.Alert("query scan error [%s]", err.Error())
			return nil, err
		}
		result = append(result, value)
		count++
	}

	return result[0:count], nil
}
func (db *RdsDb) GetMap(format string, args ...interface{}) (map[string]string, error) {
	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

type GMap map[string]string

func (db *RdsDb) GetRecordMap(format string, args ...interface{}) (GMap, error) {
	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query)
	if err != nil {
		trn.Alert("GetMap failed [%s]", err.Error())
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	result := make(GMap)

	//   columns := make([]interface{}, len(cols))
	//   columnPointers := make([]interface{}, len(cols))
	//   for i, _ := range columns {
	//       columnPointers[i] = &columns[i]
	//   }
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, _ := range columns {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {
		err := rows.Scan(columnPointers...)
		if err != nil {
			trn.Alert("scan failed")
			return nil, err
		}
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			if *val != nil {
				result[colName] = fmt.Sprintf("%v",*val)
			}
		}
	}

	return result, nil
}

func (db *RdsDb) GetRecordMapArray(format string, args ...interface{}) ([]GMap, error) {

	query := fmt.Sprintf(format, args...)
	rows, err := db.Query(query) // Note: Ignoring errors for brevity
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]GMap, 0, ARRAY_INIT)
	ctr := 0

	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.

	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, _ := range columns {
		columnPointers[i] = &columns[i]
	}

	for rows.Next() {

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(GMap)
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			if *val != nil {
				m[colName] = fmt.Sprintf("%v",*val)
			}
		}
		result = append(result, m)
		ctr++
	}
	return result, nil
}

func (db *RdsDb) Escape(input string) string {
  var result string
  for i := 0 ; i < len(input) ; i++ {
  b := input[i]
  switch {
         case b == 0:
            result += "\\"
            result += string(0)
         case b == 0x5c:
            result += string(0x5c)
            result += string(0x5c)
         case b == 0x27:
            result += string(0x5c)
            result += string(0x27)
         default:
            result += string(b)
         }
  } 
  return result
}
