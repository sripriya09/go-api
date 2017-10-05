package main

import (
	"database/sql"
	"flag"
	"fmt"
	//"reflect"
	"net/http"
	"strings"
	//"io/ioutil"
	//"encoding/json"
	//"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	password      = flag.String("password", "test123", "the database password")
	port     *int = flag.Int("port", 1433, "the database port")
	server        = flag.String("server", "localhost", "the database server")
	user          = flag.String("user", "sa", "the database user")
	database      = flag.String("database", "md", "the database name")
	metadataPath  = "D:/htdocs/metadata.json"
)

func main() {
	flag.Parse()
	
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", *server, *user, *password, *port, *database)
	conn, err := sql.Open("mssql", connString)
	checkStatus(err, "")
	defer conn.Close()

	if err == nil {
		generateMetadata(conn)

		router := gin.Default()
		
		add(conn, router)
		getAll(conn, router)
		update(conn, router)		
		delete(conn, router)
		
		router.Run()
	}
}

//CREATE
func add(conn *sql.DB, router *gin.Engine) {
	router.POST("/:table", func(c *gin.Context) {
		var message string
		table := c.Param("table")
		isTable, tableData := CheckTable(table)
		
		if isTable {
			col, val := getFormData(c)
			
			if checkFields(tableData, col) {
				keys := strings.Join(col, ",")
				ph := "?" + strings.Repeat(",?", len(val)-1)
				
				stmt, err := conn.Prepare("insert into " + table + " (" + keys + ") values (" + ph + ");")
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

//READ
func getAll(conn *sql.DB, router *gin.Engine) {
	
	router.GET("/:table", func(c *gin.Context) {
		var result gin.H
		table := c.Param("table")
		isTable, _ := CheckTable(table)
		
		if isTable {
			rows, err := conn.Query("select * from " + table)
			checkStatus(err, "")
			defer rows.Close()
			
			columns, err := rows.Columns()
			checkStatus(err, "")
			count := len(columns)
			tableData := make([]map[string]interface{}, 0)
			values := make([]interface{}, count)
			valuePtrs := make([]interface{}, count)
			
			for rows.Next() {
				for i := 0; i < count; i++ {
					valuePtrs[i] = &values[i]
				}
				rows.Scan(valuePtrs...)
				entry := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
					entry[col] = v
				}
				tableData = append(tableData, entry)
			}
			
			result = gin.H {
				"result": tableData,
				"count": len(tableData),
			}
		} else {
			result = gin.H {
				"message": "Table not found",
			}
		}
		
		c.JSON(http.StatusOK, result)
	})
}

//UPDATE FULLY
func update(conn *sql.DB, router *gin.Engine) {
	
	router.PUT("/:table", func(c *gin.Context) {
		var message string
		table := c.Param("table")
		isTable, tableData := CheckTable(table)
		
		if isTable {
			col, val := getFormData(c)
			
			if checkFields(tableData, col) {
				pk := tableData.Primary_key
				value := c.Request.URL.Query()[pk][0]
				
				data := col[0] + " = ?"
				for i := 1; i < len(col); i++ {
					data = data + ", " + col[i] + " = ?"
				}
				
				stmt, err := conn.Prepare("update " + table + " SET " + data + " where " + pk + " = " + value)
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

//DELETE
func delete(conn *sql.DB, router *gin.Engine) {
	router.DELETE("/:table", func(c *gin.Context) {
		var message string
		table := c.Param("table")
		isTable, tableData := CheckTable(table)
		
		if isTable {
			pk := tableData.Primary_key
			value := c.Request.URL.Query()[pk][0]
			
			_, err := conn.Query("delete from " + table + " where " + pk + " = " + value)
		
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

func getFormData(c *gin.Context) (col []string, val []interface{}) {
	c.Request.ParseMultipartForm(1000)
	formData := c.Request.PostForm

	for key, value := range formData {
		col = append(col, key)
		val = append(val, value[0])
	}
	
	return
}