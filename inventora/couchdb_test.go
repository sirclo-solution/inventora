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

func TestMovements(t *testing.T) {
	d := getNewDB("test_commit_posting", t)
	postings := []Posting{
		{Movements: nil},
		{Movements: map[string]float64{"Account1": 3, "Account2": -3}},
		{Movements: map[string]float64{"Account1": 5, "Account2": -5}},
		{Movements: map[string]float64{"Account1": 7, "Account2": -7}},
		{Movements: map[string]float64{"Account2": 13, "Account3": -13}},
		{Movements: map[string]float64{"Account1": -11, "Account3": 11}},
		{Movements: map[string]float64{"Account1": 23, "Account2": -12.5, "Account3": -10.5}},
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
		"Account1":          27,
		"Account2":          -14.5,
		"Account3":          -12.5,
		"InexistentAccount": 0,
	}

	for i := range expectedAccountValues {
		qty := d.AccountValue(i)
		t.Logf("Account %s expected quantity: %f actual: %f", i, expectedAccountValues[i], qty)
		if math.Abs(qty-expectedAccountValues[i]) > 0.0000001 {
			t.Fail()
		}
	}
}
