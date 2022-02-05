package dbconfig

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "root"
	dbname   = "event_demo"
)

func DBConnection() *sql.DB {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)

	if err != nil {
		log.Panic("couldn't connect to database", err)
	}

	return db
}

func SeedDB(db *sql.DB) error {
	log.Print("💾 Seeding database with table...")
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS "public"."jobs" (
			"id"      SERIAL PRIMARY KEY,
			"name"    varchar(50) NOT NULL,
			"payload" text,
			"runAt"   TIMESTAMP NOT NULL,
			"cron"    varchar(50) DEFAULT '-'
		)
	`)

	if err != nil {
		log.Panic("query error: ", err)
	}

	return err
}
