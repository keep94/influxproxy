package main

import (
	"github.com/Symantec/influxproxy/config"
	"github.com/Symantec/influxproxy/qlutils"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"io"
	"sort"
	"sync"
	"time"
)

// Single influx instance
type instance struct {
	Cl client.Client
	// The duration of this instane
	Duration time.Duration
}

// Immutable list of instances sorted from oldest to youngest
type instanceList []instance

func (l instanceList) Len() int {
	return len(l)
}

func (l instanceList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l instanceList) Less(i, j int) bool {
	// Bigger duration means going further back in time
	return l[i].Duration > l[j].Duration
}

// return the min time of instance at given index
func (l instanceList) minTime(i int, now time.Time) time.Time {
	return now.Add(-l[i].Duration)
}

// return the max time of instance at given index
func (l instanceList) maxTime(i int, now time.Time) time.Time {
	if i+1 == len(l) {
		return now
	}
	return now.Add(-l[i+1].Duration)
}

// SplitQuery splits given query across all the instances by time.
func (l instanceList) SplitQuery(
	query *influxql.Query, now time.Time) (
	splitQueries []*influxql.Query, err error) {
	if len(l) == 0 {
		return
	}
	result := make([]*influxql.Query, len(l))
	for i := range result {
		result[i], err = qlutils.QuerySetTimeRange(
			query, l.minTime(i, now), l.maxTime(i, now))
		if err != nil {
			return
		}
	}
	return result, nil
}

// executerType executes queries across multiple influx db instances.
// executerType instances are safe to use with multiple goroutines
type executerType struct {
	lock      sync.Mutex
	instances instanceList
}

// newExecuter returns a new instance with no configuration. Querying it
// will always return an empty response.
func newExecuter() *executerType {
	return &executerType{}
}

// SetupWithStream sets up this instance with config file contents in r.
func (e *executerType) SetupWithStream(r io.Reader) error {
	var cluster config.Cluster
	if err := yamlutil.Read(r, &cluster); err != nil {
		return err
	}
	newInstances := make(instanceList, len(cluster.Instances))
	for i := range newInstances {
		cl, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: cluster.Instances[i].HostAndPort,
		})
		if err != nil {
			return err
		}
		newInstances[i] = instance{
			Cl:       cl,
			Duration: cluster.Instances[i].Duration,
		}
	}
	sort.Sort(newInstances)
	e.set(newInstances)
	return nil
}

// Query runs a query against multiple influx db instances merging the results
func (e *executerType) Query(queryStr, database, epoch string) (
	*client.Response, error) {
	now := time.Now()
	query, err := qlutils.NewQuery(queryStr, now)
	if err != nil {
		return nil, err
	}
	fetchedInstances := e.get()
	querySplits, err := fetchedInstances.SplitQuery(query, now)
	if err != nil {
		return nil, err
	}

	// These are placeholders for the responses from each influx db instance
	responses := make([]*client.Response, len(querySplits))
	errs := make([]error, len(querySplits))

	var wg sync.WaitGroup
	responseIdx := 0
	for instanceIdx, querySplit := range querySplits {
		// Query not applicable to this instance, skip
		if querySplit == nil {
			continue
		}
		wg.Add(1)
		go func(
			cl client.Client,
			query string,
			responseHere **client.Response,
			errHere *error) {
			*responseHere, *errHere = cl.Query(
				client.NewQuery(query, database, epoch))
			wg.Done()
		}(fetchedInstances[instanceIdx].Cl,
			querySplit.String(),
			&responses[responseIdx],
			&errs[responseIdx])
		responseIdx++
	}
	wg.Wait()
	responses = responses[:responseIdx]
	errs = errs[:responseIdx]
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return qlutils.MergeResponses(responses...)
}

func (e *executerType) set(instances instanceList) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.instances = instances
}

func (e *executerType) get() instanceList {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.instances
}
