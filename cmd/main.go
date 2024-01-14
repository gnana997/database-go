package main

import (
	"fmt"
	"gnana997/database-go/db"
	"log"
)

func main() {
	user := map[string]string{
		"name": "Gnana",
		"age":  "25",
	}
	_ = user

	db, err := db.New()
	if err != nil {
		log.Fatal(err)
	}

	coll, err := db.CreateCollection("users")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v/n", coll)
}
