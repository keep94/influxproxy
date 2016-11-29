package main

import (
	"flag"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	"log"
	"net/http"
	"net/url"
)

func setHeader(w http.ResponseWriter, r *http.Request, key, value string) {
	r.Header.Set(key, value)
	w.Header().Set(key, value)
}

func uuidHandler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		setHeader(w, r, "Request-Id", uid.String())
		setHeader(w, r, "X-Influxdb-Version", "0.13.0")
		inner.ServeHTTP(w, r)
	})
}

type seriesListType struct {
	Series []models.Row `json:"series"`
}

type resultListType struct {
	Results []seriesListType `json:"results"`
}

func main() {
	backendAddr := flag.String("backendAddr", "", "backend address")
	flag.Parse()
	cl, err := client.NewHTTPClient(
		client.HTTPConfig{
			Addr: *backendAddr,
		})
	if err != nil {
		log.Fatal(err)
	}
	http.Handle(
		"/query",
		uuidHandler(
			apiutil.NewHandler(
				func(req url.Values) (interface{}, error) {
					resp, err := cl.Query(
						client.NewQuery(
							req.Get("q"), req.Get("db"), req.Get("epoch")))
					if err == nil {
						err = resp.Error()
					}
					if err != nil {
						return nil, err
					}
					results := &resultListType{
						Results: make([]seriesListType, len(resp.Results)),
					}
					for i := range results.Results {
						results.Results[i] = seriesListType{
							Series: resp.Results[i].Series,
						}
					}
					return results, nil
				},
				nil,
			)))
	if err := http.ListenAndServe(":8086", nil); err != nil {
		log.Fatal(err)
	}
}
