package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"

	_ "github.com/denisenkom/go-mssqldb"
)

type Metadata struct {
	DB         string
	Tables     []Table
	Tablecount int
}

type Table struct {
	Table_name   string //name of the table
	Column_count int    //number of columns in a table
	Columns      []Column
	Primary_key  string //primary key of the table
}

type Column struct {
	Column_name string
	Column_type string
}

var metadata Metadata

//checks for error
func checkError(e error) {
	if e != nil {
		fmt.Println(e.Error())
	}
}

//generates metdata for a given database db
func generateMetadata(conn *sql.DB, db string) {
	var (
		md     Metadata
		table  Table
		column Column
	)

	_, err := conn.Exec("USE " + db)
	checkError(err)

	//Query to get list of table names in a database
	tableRows, err := conn.Query("USE " + db + " SELECT table_name FROM information_schema.tables WHERE table_type = 'base table';")
	checkError(err)
	defer tableRows.Close()

	for tableRows.Next() {
		err = tableRows.Scan(&table.Table_name)
		checkError(err)

		//Query to get field names and data types in a table
		columnRows, err := conn.Query("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?;", table.Table_name)
		checkError(err)

		for columnRows.Next() {
			err = columnRows.Scan(&column.Column_name, &column.Column_type)
			table.Columns = append(table.Columns, column)
			checkError(err)
		}

		table.Column_count = len(table.Columns)

		//Query to get the primary key of a table
		pkRows, err := conn.Query("SELECT column_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name=?;", table.Table_name)
		checkError(err)

		for pkRows.Next() {
			err = pkRows.Scan(&table.Primary_key)
			checkError(err)
		}

		md.Tables = append(md.Tables, table)
		table.Columns = nil
	}

	md.DB = db
	md.Tablecount = len(md.Tables)

	writeInFile(md, db)
}

//writes the metadata in a file
func writeInFile(metadata Metadata, db string) {
	md, err := json.Marshal(metadata)
	checkError(err)

	metadataPath := metadataFolder + "/" + db + ".json"
	err = ioutil.WriteFile(metadataPath, md, 0644)
	checkError(err)
}

//reads the metadata from a file
func readFromFile(db string) (data Metadata) {
	metadataPath := metadataFolder + "/" + db + ".json"
	content, err := ioutil.ReadFile(metadataPath)
	checkError(err)

	err = json.Unmarshal(content, &data)
	checkError(err)

	return data
}

//checks with the metadata if the table is present in database
func checkTable(table string, db string) (isTable bool, tableData Table) {
	if metadata.DB != db {
		metadata = readFromFile(db)
	}

	tables := metadata.Tables

	for i := 0; i < len(tables); i++ {
		if tables[i].Table_name == table {
			tableData = tables[i]
			isTable = true
		}
	}

	return
}

//checks with the metadata if the fields are present in table
func checkFields(tableData Table, Fields []string) (isField bool) {
	columns := tableData.Columns

	isField = false

	for i := 0; i < len(Fields); i++ {
		if containsField(columns, Fields[i]) {
			isField = true
		} else {
			isField = false
			return
		}
	}
	return
}

//checks if the field is present in the column list
func containsField(list []Column, field string) (isField bool) {
	isField = false
	for _, t := range list {
		if t.Column_name == field {
			isField = true
		}
	}
	return
}
