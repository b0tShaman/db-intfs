package main

import (
	"time"

	log "github.com/sirupsen/logrus"

	"db-intfs/db"
)

// Simple struct to insert
type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
	Time int64  `json:"time"`
}

func main() {
	// Couchbase connection details
	host := "couchbase://localhost"
	username := "admin"
	password := "admin"
	bucket := "default"
	scope := "_default"
	collection := "_default"

	// Create client
	client := db.NewCouchbaseClient(host, username, password, bucket, scope, collection)
	defer client.Shutdown()

	log.Info("Connected to Couchbase!")

	// Prepare data
	user := User{
		ID:   "user::1001",
		Name: "Vishal",
		Age:  30,
		Time: time.Now().Unix(),
	}

	// Insert
	cas, err := client.Insert(user.ID, user, 0)
	if err != nil {
		log.Error("Insert error:", err)
		return
	}
	log.Info("Insert CAS:", cas)

	// Read back
	var readUser User
	_, err = client.Get(user.ID, &readUser)
	if err != nil {
		log.Error("Get error:", err)
		return
	}
	log.Info("Fetched:", readUser)
}
