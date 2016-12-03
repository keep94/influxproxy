package qlutils

import (
	"errors"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"time"
)

var (
	// means the query contains a statement that is not a select statement.
	ErrNonSelectStatement = errors.New("qlutils: Non select statement")
)

// NewQuery creates a new query instance from a string substituting currentTime
// for now().
func NewQuery(ql string, currentTime time.Time) (*influxql.Query, error) {
	return newQuery(ql, currentTime)
}

// QueryTimeRange returns the min and max time for a query. If no min time
// found, min is the zero value; if no max time found, max = now.
func QueryTimeRange(
	query *influxql.Query, now time.Time) (min, max time.Time, err error) {
	return queryTimeRange(query, now)
}

// QuerySetTimeRange returns a query just like query except that it is only
// for times falling between min inclusive and max exclusive. If none of the
// select statements in the query matches the given time range,
// returns nil, nil
func QuerySetTimeRange(
	query *influxql.Query, min, max time.Time) (*influxql.Query, error) {
	return querySetTimeRange(query, min, max)
}

// MergeResponses merges responses from multiple servers into a single
// response.
// MergeResponses expects the same query to be sent to all servers except
// for different time ranges.
// An error in any respone means an error in the merged response.
//
// The returned response will contain time series with values sorted by time
// in increasing order even if the responses merged had times in
// decreasing order.
//
// If the returned responses containing multiple series, they will be sorted
// first by name and then by the tags. When sorting tags, MergeResponses
// first places the tag keys of the time series to be sorted in ascending
// order. To compare two sets of tags, MergeResponses first compares the
// first pair of tag keys. If they match, MergeResponses uses the values of
// those first keys to break the tie. If those match, MergeResponses uses
// the second pair of tag keys to break the tie. If those match,
// MergeResponses uses the values of the second keys to brak the tie etc.
//
// MergeResponse includes all messages from the responses being merged in
// the merged response.
func MergeResponses(responses ...*client.Response) (*client.Response, error) {
	return mergeResponses(responses)
}
