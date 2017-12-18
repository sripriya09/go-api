// @APIVersion 1.0.0
// @APITitle Generic API
// @APIDescription Generic APIs for CRUD operations on all databases and tables.
// @Contact priya.star09@gmail.com
// @TermsOfServiceUrl http://google.com/
// @License BSD
// @LicenseUrl http://opensource.org/licenses/BSD-2-Clause
// @BasePath http://localhost:8080/

// @SubApi Table [/:db/:table]
// @SubApi Allows access to given table in a given database [/:db/:table]

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gin-gonic/gin"
)

var (
	password            = flag.String("password", "test123", "the database password")
	port           *int = flag.Int("port", 1433, "the database port")
	server              = flag.String("server", "localhost", "the database server")
	user                = flag.String("user", "sa", "the database user")
	database            = flag.String("database", "goDB", "the database name")
	metadataFolder      = "D:/htdocs/metadata"
)

func main() {
	flag.Parse()

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", *server, *user, *password, *port)
	fmt.Println(connString)
	conn, err := sql.Open("mssql", connString)
	checkError(err)
	defer conn.Close()

	if err == nil {
		DBRouter := gin.New()
		getDB(conn, DBRouter)
		go DBRouter.Run(":8090")

		ApiRouter := gin.Default()
		addRecord(conn, ApiRouter)
		getRecords(conn, ApiRouter)
		updateRecord(conn, ApiRouter)
		deleteRecord(conn, ApiRouter)
		ApiRouter.Run()
	}
}

//API to get the list of databases in server

// @Title getDatabases
// @Description retrieves all database names in a given server
// @Produce  json
// @Success 200 {object} gin.H "Success. array of database names in a server is fetched"
// @Router /databases [get]

func getDB(conn *sql.DB, router *gin.Engine) {
	dbRows, err := conn.Query("SELECT name FROM sys.databases")
	checkError(err)
	defer dbRows.Close()

	columns, err := dbRows.Columns()
	dbCount := len(columns)
	var dbArray []string
	values := make([]string, dbCount)
	valuePtrs := make([]interface{}, dbCount)

	for dbRows.Next() {
		for i := 0; i < dbCount; i++ {
			valuePtrs[i] = &values[i]
		}
		dbRows.Scan(valuePtrs...)
		for j := 0; j < dbCount; j++ {
			dbArray = append(dbArray, values[j])
			generateMetadata(conn, values[j])
		}
	}

	router.GET("/databases", func(c *gin.Context) {
		result := gin.H{
			"databases": dbArray,
			"count":     len(dbArray),
		}

		c.JSON(http.StatusOK, result)
	})
}

//CREATE API - Inserts the record in table

// @Title addRecord
// @Description Inserts record into a given table in a given database
// @Accept  json
// @Param   db  	path    string     true        "Database Name"
// @Param   table 	path   	string     true        "Table Name"
// @Success 200 {object} gin.H	"Success. Record added successfully."
// @Failure 404 {object} gin.H  "Given database or table or field not found"
// @Resource /:db/:table
// @Router / [post]

func addRecord(conn *sql.DB, router *gin.Engine) {
	router.POST("/:db/:table", func(c *gin.Context) {
		var message string
		var status int
		table := c.Param("table")
		db := c.Param("db")
		isTable, tableData := checkTable(table, db)

		if isTable {
			col, val := getFormData(c)

			if checkFields(tableData, col) {
				keys := strings.Join(col, ",")
				ph := "?" + strings.Repeat(",?", len(val)-1)

				stmt, err := conn.Prepare("use " + db + " insert into " + table + " (" + keys + ") values (" + ph + ");")
				_, err = stmt.Exec(val...)

				if err != nil {
					message = err.Error()
				} else {
					status = http.StatusOK
					message = "Record added Successfully"
				}
			} else {
				status = http.StatusNotFound
				message = "Field name not found in table"
			}
		} else {
			message = "Table not found"
			status = http.StatusNotFound
		}

		c.JSON(status, gin.H{
			"message": fmt.Sprintf(message),
		})
	})
}

//READ API - reads all the records from a given table

// @Title readRecords
// @Description Retrieves records from a given table in a given database
// @Produce  json
// @Param   db  	path    string     true        "Database Name"
// @Param   table 	path   	string     true        "Table Name"
// @Success 200 {object} gin.H	"Success. Records from given table are fetched"
// @Failure 404 {object} gin.H  "Given database or table not found"
// @Resource /:db/:table
// @Router / [get]

func getRecords(conn *sql.DB, router *gin.Engine) {

	router.GET("/:db/:table", func(c *gin.Context) {
		var status int
		var result gin.H
		table := c.Param("table")
		db := c.Param("db")
		isTable, _ := checkTable(table, db)

		if isTable {
			recordRows, err := conn.Query("use " + db + " select * from " + table)
			checkError(err)
			defer recordRows.Close()

			columns, err := recordRows.Columns()
			checkError(err)
			recordCount := len(columns)
			tableData := make([]map[string]interface{}, 0)
			values := make([]interface{}, recordCount)
			valuePtrs := make([]interface{}, recordCount)

			for recordRows.Next() {
				for i := 0; i < recordCount; i++ {
					valuePtrs[i] = &values[i]
				}
				recordRows.Scan(valuePtrs...)
				record := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
					record[col] = v
				}
				tableData = append(tableData, record)
			}

			status = http.StatusOK
			result = gin.H{
				"result": tableData,
				"count":  len(tableData),
			}
		} else {
			status = http.StatusNotFound
			result = gin.H{
				"message": "Table not found",
			}
		}

		c.JSON(status, result)
	})
}

//UPDATE API - updates the entire record in a given table

// @Title updateRecord
// @Description Updates record of a given table in a given database
// @Accept  json
// @Param   db  			path    string     	true        "Database Name"
// @Param   table 			path   	string		true        "Table Name"
// @Param   primary_key 	path   	int     	true        "Primary Key"
// @Success 200 {object} gin.H	"Success. Record updated successfully"
// @Failure 404 {object} gin.H  "Given database or table or field not found"
// @Resource /:db/:table
// @Router / [put]

func updateRecord(conn *sql.DB, router *gin.Engine) {

	router.PUT("/:db/:table", func(c *gin.Context) {
		var status int
		var message string
		table := c.Param("table")
		db := c.Param("db")
		isTable, tableData := checkTable(table, db)

		if isTable {
			col, val := getFormData(c)

			if checkFields(tableData, col) {
				pk := tableData.Primary_key
				pkValue := c.Request.URL.Query()[pk][0]

				data := col[0] + " = ?"
				for i := 1; i < len(col); i++ {
					data = data + ", " + col[i] + " = ?"
				}

				stmt, err := conn.Prepare("use " + db + " update " + table + " SET " + data + " where " + pk + " = " + pkValue)
				_, err = stmt.Exec(val...)

				if err != nil {
					message = err.Error()
				} else {
					status = http.StatusOK
					message = "Record updated Successfully"
				}
			} else {
				status = http.StatusNotFound
				message = "Field name not found in table"
			}
		} else {
			status = http.StatusNotFound
			message = "Table not found"
		}

		c.JSON(status, gin.H{
			"message": fmt.Sprintf(message),
		})
	})
}

//DELETE API - Deletes a particular record in a given table

// @Title deleteRecord
// @Description Deletes record in a given table in a given database
// @Param   db  			path    string     	true        "Database Name"
// @Param   table 			path   	string		true        "Table Name"
// @Param   primary_key 	path   	int     	true        "Primary Key"
// @Success 200 {object} "Success. Record deleted successfully"
// @Failure 404 {object} "Given database or table not found"
// @Resource /:db/:table
// @Router / [delete]

func deleteRecord(conn *sql.DB, router *gin.Engine) {
	router.DELETE("/:db/:table", func(c *gin.Context) {
		var status int
		var message string
		table := c.Param("table")
		db := c.Param("db")
		isTable, tableData := checkTable(table, db)

		if isTable {
			pk := tableData.Primary_key
			pkValue := c.Request.URL.Query()[pk][0]

			_, err := conn.Query("use " + db + " delete from " + table + " where " + pk + " = " + pkValue)

			if err != nil {
				message = err.Error()
			} else {
				status = http.StatusOK
				message = "Record Deleted Successfully"
			}
		} else {
			status = http.StatusNotFound
			message = "Table not found"
		}

		c.JSON(status, gin.H{
			"message": fmt.Sprintf(message),
		})
	})
}

//returns key array and value array of form data
func getFormData(c *gin.Context) (col []string, val []interface{}) {
	c.Request.ParseMultipartForm(1000)
	formData := c.Request.PostForm

	for key, value := range formData {
		col = append(col, key)
		val = append(val, value[0])
	}

	return
}
