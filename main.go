package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

const notifyBootStatusWebhookUrl string = "<WEBHOOK_URL>"
const notifyCatLocationWebhookUrl string = "<WEBHOOK_URL>"
const roomCount int = 3

type Device struct {
	RoomName string `json:"roomName" binding:"required"`
}

type CalcLastPosition struct {
	RoomName string         `db:"roomname"`
	AvgRssi  float64        `db:"avgRssi"`
	LastRoom sql.NullString `db:"lastroom"`
}

type NotifyBoot struct {
	RoomName string `json:"value1"`
}

type NotifyCat struct {
	Text string `json:"value1"`
}

func notifyBootStatusFunc(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		macAddress := c.Query("macAddress")

		var device Device
		if err := db.QueryRow("SELECT roomName FROM devices WHERE macAddress = $1 LIMIT 1", macAddress).Scan(&device.RoomName); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		jsonData, err := json.Marshal(NotifyBoot{RoomName: device.RoomName})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		post(notifyBootStatusWebhookUrl, jsonData)

		c.Status(http.StatusOK)
	}
}

func post(url string, jsonData []byte) error {
	request, error := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	client := &http.Client{}
	response, error := client.Do(request)
	if error != nil {
		return error
	}

	defer response.Body.Close()

	return nil
}

func updateBeaconFunc(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		macAddress := c.Query("macAddress")
		var rssiArrayRaw []string
		if c.Query("rssi") != "" {
			rssiArrayRaw = strings.Split(strings.TrimRight(c.Query("rssi"), ","), ",")
		} else {
			rssiArrayRaw = []string{}
		}

		rssiArray := make([]int, len(rssiArrayRaw))

		for i := 0; i < len(rssiArrayRaw); i++ {
			rssiArray[i], _ = strconv.Atoi(rssiArrayRaw[i])
		}

		if macAddress == "" {
			c.Status(http.StatusBadRequest)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		defer func() {
			if recover() != nil {
				tx.Rollback()
			}
		}()

		if _, err := tx.Exec("LOCK TABLE beacon IN EXCLUSIVE MODE"); err != nil {
			tx.Rollback()
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error lock db: %q", err))
			return
		}

		var device Device
		if err := tx.QueryRow("SELECT roomName FROM devices WHERE macAddress = $1 LIMIT 1", macAddress).Scan(&device.RoomName); err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if _, err := tx.Exec("DELETE FROM beacon WHERE roomname = $1", device.RoomName); err != nil {
			tx.Rollback()
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error clean beacon database: %q", err))
			return
		}

		if len(rssiArray) != 0 {
			for i := 0; i < len(rssiArray); i++ {
				if _, err := tx.Exec("INSERT INTO beacon VALUES($1, $2, null, now())", device.RoomName, rssiArray[i]); err != nil {
					tx.Rollback()
					c.String(http.StatusInternalServerError,
						fmt.Sprintf("Error insert beacon table: %q", err))
					return
				}
			}
		} else {
			if _, err := tx.Exec("INSERT INTO beacon VALUES($1, $2, null, now())", device.RoomName, -9999); err != nil {
				tx.Rollback()
				c.String(http.StatusInternalServerError,
					fmt.Sprintf("Error insert beacon table: %q", err))
				return
			}
		}

		rows, err := tx.Query("SELECT count(*) FROM beacon WHERE created_at >= (now() + '-1 minute') AND created_at <= (now() + '+1 minute') GROUP BY roomname")
		if err != nil {
			tx.Rollback()
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error fetch roomName count: %q", err))
			return
		}

		rowsRoomCount := 0
		for rows.Next() {
			rowsRoomCount += 1
		}

		if rowsRoomCount != roomCount {
			tx.Commit()
			return
		}

		var result CalcLastPosition
		if resultErr := tx.QueryRow("SELECT roomname, AVG(rssi) as avgRssi, (SELECT roomname FROM lastPosition LIMIT 1) as lastroom FROM beacon GROUP BY roomname ORDER BY avgRssi DESC LIMIT 1").Scan(&result.RoomName, &result.AvgRssi, &result.LastRoom); resultErr != nil {
			tx.Rollback()
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error fetch last position: %q", resultErr))
			return
		}

		if result.AvgRssi == -9999 {
			if result.LastRoom.String != "" {
				jsonData, err := json.Marshal(NotifyCat{Text: "ﾈｺﾁｬﾝを見失いました…\n最後にいた場所: " + result.LastRoom.String})
				if err == nil {
					post(notifyCatLocationWebhookUrl, jsonData)
				}

				tx.Exec("DELETE FROM lastPosition")
			}
		} else if result.RoomName != result.LastRoom.String {
			var action string
			if result.LastRoom.String == "" {
				action = "見つかりました"
			} else {
				action = "移動しました"
			}

			jsonData, err := json.Marshal(NotifyCat{Text: "ﾈｺﾁｬﾝが" + action + ": " + result.RoomName + "\nRSSI平均値: " + strconv.FormatFloat(result.AvgRssi, 'f', -1, 64)})
			if err == nil {
				post(notifyCatLocationWebhookUrl, jsonData)
			}

			tx.Exec("DELETE FROM lastPosition")

			if _, err := tx.Exec("INSERT INTO lastPosition(roomname) VALUES($1)", result.RoomName); err != nil {
				tx.Rollback()
				c.String(http.StatusInternalServerError,
					fmt.Sprintf("Error insert lastPosition: %q", err))
				return
			}
		}

		tx.Commit()
	}
}

func main() {
	port := os.Getenv("PORT")
	dbUrl := os.Getenv("DATABASE_URL")

	if port == "" || dbUrl == "" {
		log.Fatal("$PORT/$DATABASE_URL must be set")
	}

	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("Error opening database: %q", err)
	}

	router := gin.New()
	router.Use(gin.Logger())

	router.GET("/", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	router.POST("/beacon", updateBeaconFunc(db))
	router.POST("/notify/boot", notifyBootStatusFunc(db))

	router.Run(":" + port)
}
