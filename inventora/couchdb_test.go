package inventora

import (
	"math"
	"testing"

	"github.com/zemirco/couchdb"
)

const dbURL = "http://localhost:5984/"

func getNewDB(dbName string, t *testing.T) *Database {
	client, _ := couchdb.NewClient(dbURL)
	client.Delete(dbName)

	d, err := New(dbURL, dbName)
	if err != nil {
		t.Error(err)
	}
	return d
}

func TestGetMultipleDB(t *testing.T) {
	for i := 0; i < 3; i++ {
		_, err := New(dbURL, "test_db")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestSimpleCommitPosting(t *testing.T) {
	d := getNewDB("test_simple_commit_posting", t)
	posting := Posting{Movements: []Movement{
		{[]string{"Account1"}, 10.12},
		{[]string{"Account2"}, -10.12},
	}}
	err := d.CommitPosting(&posting)
	t.Log(err)
	if err != nil {
		t.Error(err)
	}
}

func TestMovements(t *testing.T) {
	d := getNewDB("test_commit_posting", t)
	postings := []Posting{
		{Movements: nil},
		{Movements: []Movement{{[]string{"Account", "1"}, 3}, {[]string{"Account", "2"}, -3}}},
		{Movements: []Movement{{[]string{"Account", "1"}, 5}, {[]string{"Account", "2"}, -5}}},
		{Movements: []Movement{{[]string{"Account", "1"}, 7}, {[]string{"Account", "2"}, -7}}},
		{Movements: []Movement{{[]string{"Account", "2"}, 13}, {[]string{"Account", "3"}, -13}}},
		{Movements: []Movement{{[]string{"Account", "1"}, -11}, {[]string{"Account", "3"}, 11}}},
		{Movements: []Movement{{[]string{"Account", "1"}, 23}, {[]string{"Account", "2"}, -12.5}, {[]string{"Account", "3"}, -10.5}}},
	}
	done := make(chan bool, len(postings))
	for i := range postings {
		// Test concurrent movements
		go func(p *Posting) {
			err := d.CommitPosting(p)
			if err != nil {
				t.Error(err)
			}
			done <- true
		}(&postings[i])
	}

	for i := 0; i < len(postings); i++ {
		<-done
	}

	expectedAccountValues := map[string]float64{
		"1":          27,
		"2":          -14.5,
		"3":          -12.5,
		"Inexistent": 0,
	}

	for i := range expectedAccountValues {
		qty := d.AccountValue([]string{"Account", i})
		t.Logf("Account %s expected quantity: %f actual: %f", i, expectedAccountValues[i], qty)
		if math.Abs(qty-expectedAccountValues[i]) > 0.0000001 {
			t.Fail()
		}
	}

	totalQty := d.AccountValue([]string{"Account"})
	if math.Abs(totalQty) > 0.0000001 {
		t.Fail()
	}

}
