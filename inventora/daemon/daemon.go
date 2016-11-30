package daemon

import (
	"database/sql"
	"log"
	"time"

	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
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
	// dbMapLock sync.RWMutex // TODO
}

func New() Daemon {
	return Daemon{
		databases: make(map[string]*sql.DB),
	}
}

func (d Daemon) getDB(name string) *sql.DB {
	db := d.databases[name]
	if db != nil {
		return db
	}

	db, err := sql.Open("sqlite3",
		"./"+name+".db")
	if err != nil {
		log.Fatal(err)
	}
	db.Exec(`CREATE TABLE IF NOT EXISTS posting (
		ID INTEGER PRIMARY KEY,
		CreationInstant INT,
		Tags TEXT
	)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS movements (
		ID INTEGER PRIMARY KEY,
		PostingID INT,
		AccountID TEXT,
		Quantity INT
	)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS movements_PostingID ON movements (PostingID)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS movements_AccountID ON movements (AccountID)`)

	d.databases[name] = db
	return db
}

func (d Daemon) CommitPosting(posting *Posting) error {
	log.Printf("Putting %+v", posting)
	db := d.getDB(posting.DBName)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	marshaledTags, _ := json.Marshal(posting.Tags)
	result, err := tx.Exec("INSERT INTO posting (CreationInstant, Tags) VALUES (?, ?)", time.Now().Unix(), marshaledTags)
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

func (d Daemon) AccountValue(dbName string, accountID string) (int64, error) {
	db := d.getDB(dbName)
	var quantity *int64
	err := db.QueryRow("SELECT SUM(Quantity) FROM movements WHERE AccountID = ?", accountID).Scan(&quantity)
	if err != nil || quantity == nil {
		return 0, err
	}
	return *quantity, err
}

func (d Daemon) RegisterAccountChangeHook(dbName string, accountID string, fn func(accountID string, lastPostingID string, lastPostingInstant int64)) {
	// TODO
}
