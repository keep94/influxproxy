package main

import (
	"fmt"
	"github.com/Symantec/influxproxy/qlutils"
	"log"
	"os"
	"time"
)

func main() {
	now := time.Now()
	q, err := qlutils.NewQuery(os.Args[1], now)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(qlutils.QueryTimeRange(q, now))
	fmt.Println(qlutils.QuerySetTimeRange(q, now.Add(-2*time.Hour), now))
}
