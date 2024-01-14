package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

func main() {
	db, err := bbolt.Open(".db", 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	user := map[string]string{
		"name": "Gnana",
		"age":  "25",
	}

	db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("Users"))
		if err != nil {
			return err
		}

		uuid := uuid.New()

		for k, v := range user {
			if err := bucket.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}

		if err := bucket.Put([]byte("id"), []byte(uuid.String())); err != nil {
			return err
		}

		return nil
	})

	fmt.Println("bbolt is working")
}
