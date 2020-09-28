package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"log"
	"net/http"
	"strconv"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := gin.Default()
	config := GetConfig()
	db, err := CreateYdbClient(ctx, config.Ydb)
	if err != nil {
		log.Fatal("Could not create YDB driver: ", err)
	}
	defer db.Close()

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": "hello world"})
	})

	r.GET("/project/:id", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		_, res, err := db.Execute(`DECLARE $id AS "Uint64"; SELECT * FROM project WHERE id = $id`, table.NewQueryParameters(table.ValueParam("$id", ydb.Uint64Value(id))))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
		var projects Projects
		err = (&projects).Scan(res)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err})
			return
		}
		c.JSON(http.StatusOK, projects)
	})

	if err = r.Run("0.0.0.0:8050"); err != nil {
		log.Fatal(err)
	}
}
