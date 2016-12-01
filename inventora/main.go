package main

import (
	"log"

	daemon2 "github.com/sirclo-solution/inventora/couchdb"
	inv "github.com/sirclo-solution/inventora/daemon"
)

func main() {
	d, err := daemon2.New("http://localhost:5984/", "hello2")
	log.Printf(": %+v %+v", d, err)
	// var i uint64
	// for i = 9; i <= 1000; i++ {
	// 	log.Printf("%d %s %v", i, counterToID(i), idToCounter(counterToID(i)))
	// }
	posting := daemon2.Posting{Movements: map[string]float64{
		"Account1": 10.12,
		"Account2": -10.12,
	}}
	err = d.CommitPosting(&posting)
	log.Printf("CommitPosting: %+v", err)
	log.Printf("%f", d.AccountValue("Account1"))
	log.Printf(": %+v %+v", d, err)
}

func main2() {
	d := inv.New()
	posting := inv.Posting{DBName: "abc2", Movements: map[string]int64{
		"Account1": 10,
		"Account2": -10,
	}}

	concurrency := 100
	done := make(chan bool, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			err := d.CommitPosting(&posting)
			log.Printf("CommitPosting %d: %+v", i, err)
			done <- true
		}(i)
	}
	for i := 0; i < concurrency; i++ {
		<-done
	}
	qty, err := d.AccountValue("abc2", "Account1")
	log.Printf("err: %+v qty: %d", err, qty)
}
