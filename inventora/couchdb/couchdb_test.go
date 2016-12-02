package daemon2

import "testing"

const dbURL = "http://localhost:5984/"

func getNewDB(dbName string, t *testing.T) *Database {
	// client, _ := couchdb.NewClient(url)
	// client.Delete(dbName)

	d, err := New(dbURL, dbName)
	if err != nil {
		t.Error(err)
	}
	return d
}

func TestSimpleCommitPosting(t *testing.T) {
	d := getNewDB("test_simple_commit_posting", t)
	posting := Posting{Movements: map[string]float64{
		"Account1": 10.12,
		"Account2": -10.12,
	}}
	err := d.CommitPosting(&posting)
	t.Log(err)
	if err != nil {
		t.Error(err)
	}
}

func TestGetMultipleDB(t *testing.T) {
	for i := 0; i < 3; i++ {
		_, err := New(dbURL, "test_db")
		if err != nil {
			t.Error(err)
		}
	}
}
