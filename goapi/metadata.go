package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"encoding/json"
	//"errors"
	//"reflect"

	//"github.com/gin-gonic/gin"
	_ "github.com/denisenkom/go-mssqldb"
)

type Metadata struct {
	DB 			string
	Tables		[]Table
	Tablecount	int
}

type Table struct {
	Table_name			string
	Column_count 		int
	Columns				[]Column
	Primary_key			string
}

type Column struct {
	Column_name 	string
	Column_type		string
}

var metadata Metadata

func checkStatus(e error, msg string) {
	if e != nil {
		fmt.Println(e.Error())
	} else if msg != "" {
		fmt.Println(msg)
	}
}

func generateMetadata(conn *sql.DB, db string) {
	var (
		md Metadata
		table Table
		column Column
	)
	
	//tablecount := getTableCount(conn)
	
	_, err := conn.Exec("USE " + db)
	checkStatus(err, "")
	
	rows, err := conn.Query("USE " + db + " SELECT table_name FROM information_schema.tables WHERE table_type = 'base table';")
			
	for rows.Next() {
		err = rows.Scan(&table.Table_name)
		checkStatus(err, "")
		
		row1, err1 := conn.Query("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?;", table.Table_name)
		
		for row1.Next() {
			err1 = row1.Scan(&column.Column_name, &column.Column_type)
			table.Columns = append(table.Columns, column)
			checkStatus(err1, "")	
		}
		
		table.Column_count = len(table.Columns)
		
		row2, err2 := conn.Query("SELECT column_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND KU.table_name=?;", table.Table_name)
		
		for row2.Next() {
			err2 = row2.Scan(&table.Primary_key)
			checkStatus(err2, "")
		}
		
		/*row3, err3 := conn.Query("SELECT count(*) from " + table.Table_name)
		
		for row3.Next() {
			err3 = row3.Scan(&table.Number_of_records)
			checkStatus(err3, "")
		}*/
		
		md.Tables = append(md.Tables, table)
		table.Columns = nil
	}
	
	md.DB = db
	md.Tablecount = len(md.Tables)
	
	
	defer rows.Close()
	
	/*metadata := gin.H {
		"DB": db,
		"Tablecount": len(tables),
		"Tables": tables,
	}*/
	
	writeInFile(md, db)
}

func writeInFile(metadata Metadata, db string) {
	md, err := json.Marshal(metadata)
	checkStatus(err, "")
	
	metadataPath := metadataFolder + "/" + db + ".json"
	err = ioutil.WriteFile(metadataPath, md, 0644)
	checkStatus(err, "")
}

func readFromFile(db string) (data Metadata) {
	metadataPath := metadataFolder + "/" + db + ".json"
	content, err := ioutil.ReadFile(metadataPath)
	checkStatus(err, "")
	
	err = json.Unmarshal(content, &data)
	checkStatus(err, "")
	
	return data
}

func CheckTable(table string, db string) (isTable bool, tableData Table) {
	if (metadata.DB != db) {
		metadata  = readFromFile(db)
	} 
	
	data := metadata.Tables
	
	for i := 0; i < len(data); i++ {
		if data[i].Table_name == table {
			tableData = data[i]
			isTable = true
		}
	}
	
	return
}

func checkFields(tableData Table, Fields []string) (isField bool) {
	data := tableData.Columns
	
	isField = false
	
	for i := 0; i < len(Fields); i++ {
		if(Contains(data, Fields[i])) {
			isField = true
		} else {
			isField = false
			return 
		}
	}
	return
}

func Contains(list []Column, elem string) (isField bool) { 
	isField = false
	for _, t := range list {
		if t.Column_name == elem {
			isField = true
		} 
	} 
	return
} 

/*func getTableCount(conn *sql.DB) (count int) {

	rows, err := conn.Query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dbo';")
	checkStatus(err, "")
	
 	for rows.Next() {
    	err = rows.Scan(&count)
    	checkStatus(err, "")
    }   
    return count
}*/
 