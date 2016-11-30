package daemon

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Posting struct {
	ID              uint64
	CreationInstant int64
	DBName          string
	Movements       map[string]int64
	Tags            map[string]string
}

type Daemon struct {
	databases map[string]*sql.DB
	dbMapLock sync.RWMutex
}

func New() Daemon {
	return Daemon{
		databases: make(map[string]*sql.DB),
		dbMapLock: sync.RWMutex{},
	}
}

func (d *Daemon) getDB(name string) *sql.DB {
	d.dbMapLock.RLock()
	db := d.databases[name]
	if db != nil {
		d.dbMapLock.RUnlock()
		return db
	}
	d.dbMapLock.RUnlock()

	d.dbMapLock.Lock()
	log.Println("Acquired lock")
	defer d.dbMapLock.Unlock()

	// Try to acquire again, in case the connection has been acquired by another goroutine
	db = d.databases[name]
	if db != nil {
		return db
	}

	// log.Println("DB not found in memory, opening new one")

	db, err := sql.Open("mysql", "root@/"+name)
	if err != nil {
		log.Fatal(err)
	}
	db.Exec(`CREATE TABLE IF NOT EXISTS posting (
		ID INTEGER AUTO_INCREMENT PRIMARY KEY,
		CreationInstant INT,
		Tags TEXT
	)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS movements (
		ID INTEGER AUTO_INCREMENT PRIMARY KEY,
		PostingID INT,
		AccountID TEXT,
		Quantity INT
	)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS movements_PostingID ON movements (PostingID)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS movements_AccountID ON movements (AccountID)`)

	d.databases[name] = db
	return db
}

func (d *Daemon) CommitPosting(posting *Posting) error {
	db := d.getDB(posting.DBName)
	log.Println("DB get")
	// t := time.Now()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	marshaledTags, _ := json.Marshal(posting.Tags)
	// tm := time.Now()
	result, err := tx.Exec("INSERT INTO posting (CreationInstant, Tags) VALUES (?, ?)", time.Now().Unix(), marshaledTags)
	// log.Printf("posting: exec in %f sec", time.Since(tm).Seconds())
	if err != nil {
		return err
	}

	postingID, err := result.LastInsertId()
	if err != nil {
		return err
	}

	for accountID := range posting.Movements {
		_, err = tx.Exec("INSERT INTO movements (PostingID, AccountID, Quantity) VALUES (?, ?, ?)", postingID, accountID, posting.Movements[accountID])
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}

func (d *Daemon) AccountValue(dbName string, accountID string) (int64, error) {
	db := d.getDB(dbName)
	var quantity *int64
	err := db.QueryRow("SELECT SUM(Quantity) FROM movements WHERE AccountID = ?", accountID).Scan(&quantity)
	if err != nil || quantity == nil {
		return 0, err
	}
	return *quantity, err
}

func (d *Daemon) RegisterAccountChangeHook(dbName string, accountID string, fn func(accountID string, lastPostingID string, lastPostingInstant int64)) {
	// TODO
}
