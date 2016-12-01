package daemon2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/zemirco/couchdb"
)

const ddocName = "postings"

type Posting struct {
	couchdb.Document
	CreationInstant int64
	DBName          string
	Movements       map[string]float64
	Tags            map[string]string
}

type Database struct {
	db             couchdb.Database
	name           string
	idCounter      uint64
	idClounterLock sync.Mutex
}

func New(dbName string) (*Database, error) {
	client, err := couchdb.NewClient("http://127.0.0.1:5984/")
	d := Database{name: dbName}
	if err != nil {
		return nil, err
	}
	_, err = client.Info()
	if err != nil {
		return nil, err
	}
	// log.Printf("%+v", server)

	_, err = client.Create(dbName)
	d.db = client.Use(dbName)
	if err == nil {
		// Creating new database, so we initialize the design docs
		ddoc := couchdb.DesignDocument{
			Language: "javascript",
			Views: map[string]couchdb.DesignDocumentView{
				"lastID": couchdb.DesignDocumentView{
					Map: `
					function (doc) {
						emit(null, doc._id);
					}
					`,
					Reduce: `
					function (keys, values, rereduce) {
						return values.reduce(function(a, b) { return a > b ? a : b }, "");
					}
					`,
				},
				"movements": couchdb.DesignDocumentView{
					Map: `
					function (doc) {
						if (doc.Movements) {
							for (var k in doc.Movements) {
								var i = +doc.Movements[k]
								emit(k, isNaN(i) ? 0 : i)
							}
						}
					}
					`,
					Reduce: "_sum",
				},
			},
		}
		ddoc.ID = "_design/" + ddocName
		d.db.Put(&ddoc)
	} else {
		// Database already exists, so we get the last ID from existing reduce function.
		d.idCounter = d.lastIDForCounter() + 1
	}
	err = nil

	return &d, err
}

func (d *Database) incrementID() uint64 {
	d.idClounterLock.Lock()
	defer d.idClounterLock.Unlock()
	i := d.idCounter
	d.idCounter++
	return i
}

func (d *Database) lastIDForCounter() uint64 {
	view := d.db.View(ddocName)
	itsTrue := true
	response, _ := view.Get("lastID", couchdb.QueryParameters{
		Reduce: &itsTrue,
	})
	if response != nil && len(response.Rows) > 0 {
		return idToCounter(response.Rows[0].Value.(string))
	}
	return 0
}

func (d *Database) CommitPosting(posting *Posting) error {
	tries := 0
	var err error
	for tries < 5 {
		nextID := d.incrementID()
		posting.ID = counterToID(nextID)
		_, err = d.db.Put(posting)
		if err != nil {
			err2 := err.(*couchdb.Error)
			if err2.StatusCode != 409 {
				// This is not a doc update conflict, so we increment the try count. Doc update conflict can happen as many times as required, but other kinds of error are limited to 5 tries.
				tries++
			}
			continue
		}
		return nil
	}
	return err
}

func counterToID(v uint64) string {
	// TODO: use more compact formatting, base64?
	s := strconv.FormatUint(v, 36)
	// To make base36 lexicographically sorted (needed for couchdb reduce function) we prepend the key with the length of the string s, so that longer strings are sorted after shorter strings.
	return fmt.Sprintf("%s%s", strconv.FormatInt(int64(len(s)-1), 36), s)
}

func idToCounter(s string) uint64 {
	i, _ := strconv.ParseUint(s[1:], 36, 64)
	return i
}

func (d *Database) AccountValue(accountID string) float64 {
	view := d.db.View(ddocName)
	s, _ := json.Marshal(accountID)
	accountIDJSONEncoded := string(s)
	itsTrue := true
	response, _ := view.Get("movements", couchdb.QueryParameters{
		Group:  &itsTrue,
		Reduce: &itsTrue,
		Key:    &accountIDJSONEncoded,
	})
	// log.Println("---", response, err)
	if response != nil && len(response.Rows) > 0 {
		return response.Rows[0].Value.(float64)
	}
	return 0
}

func (d *Database) RegisterAccountChangeHook(accountID string, fn func(accountID string, lastPostingID string, lastPostingInstant int64)) {
	// TODO
}
