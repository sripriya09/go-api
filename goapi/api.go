package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	password      = flag.String("password", "test123", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "localhost", "the database server")
	user          = flag.String("user", "sa", "the database user")
	database      = flag.String("database", "md", "the database name")
	metadataFolder = "D:/htdocs/metadata"
)

func main() {
	flag.Parse()
	
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", *server, *user, *password, *port)
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
		result := gin.H {
			"databases": dbArray,
			"count": dbCount,
		}
		
		c.JSON(http.StatusOK, result)
	})			
}

//CREATE API - Inserts the record in table
func addRecord(conn *sql.DB, router *gin.Engine) {
	router.POST("/:db/:table", func(c *gin.Context) {
		var message string
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
					message = "Record added Successfully"
				}
			} else {
				message = "Field name not found in table"
			}
		} else {
			message = "Table not found"
		}
		
		c.JSON(http.StatusOK, gin.H{
			"message": fmt.Sprintf(message),
		})
	})
}

//READ API - reads all the records from a given table
func getRecords(conn *sql.DB, router *gin.Engine) {
	
	router.GET("/:db/:table", func(c *gin.Context) {
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
			
			result = gin.H {
				"result": tableData,
				"count": recordCount,
			}
		} else {
			result = gin.H {
				"message": "Table not found",
			}
		}
		
		c.JSON(http.StatusOK, result)
	})
}

//UPDATE API - updates the entire record in a given table
func updateRecord(conn *sql.DB, router *gin.Engine) {
	
	router.PUT("/:db/:table", func(c *gin.Context) {
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
					message = "Record updated Successfully"
				}
			} else {
				message = "Field name not found in table"
			}
		} else {
			message = "Table not found"
		}
		
		c.JSON(http.StatusOK, gin.H{
			"message": fmt.Sprintf(message),
		})
	})
}

//DELETE API - Deletes a particular record in a given table
func deleteRecord(conn *sql.DB, router *gin.Engine) {
	router.DELETE("/:db/:table", func(c *gin.Context) {
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
				message = "Record Deleted Successfully"
			}
		} else {
			message = "Table not found"
		}
		
		c.JSON(http.StatusOK, gin.H{
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