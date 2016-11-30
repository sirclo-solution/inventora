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
	err := d.CommitPosting(&posting)
	log.Printf("%+v", err)
	qty, err := d.AccountValue("abc2", "Account1")
	log.Printf("%+v", err)
	log.Print(qty)
}
