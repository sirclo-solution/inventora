package main

import (
	"log"

	inv "github.com/sirclo-solution/inventora/daemon"
)

func main() {
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
